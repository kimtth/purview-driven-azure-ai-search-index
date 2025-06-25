import asyncio
import os
from enum import Enum
from loguru import logger
from typing import List, Dict, Any

from azure.identity import DefaultAzureCredential
from azure.purview.catalog import PurviewCatalogClient  # old
from azure.purview.datamap import DataMapClient  # new
from azure.core.exceptions import AzureError

from azure.purview.scanning import PurviewScanningClient
from azure.identity import ClientSecretCredential
from azure.core.exceptions import HttpResponseError
from azure.purview.administration.account import PurviewAccountClient

from dotenv import load_dotenv
from typing import Optional

import requests

load_dotenv(verbose=True)


client_id = os.getenv("AZURE_CLIENT_ID")
client_secret = os.getenv("AZURE_CLIENT_SECRET")
tenant_id = os.getenv("AZURE_TENANT_ID")
purview_acocunt_name = os.getenv("PURVIEW_ACCOUNT_NAME")
purview_endpoint = os.getenv("PURVIEW_ENDPOINT")
purview_account_endpoint = os.getenv("PURVIEW_ACCOUNT_ENDPOINT")
purview_scan_endpoint = os.getenv("PURVIEW_SCAN_ENDPOINT")


class AzureSearchDataType(Enum):
    EDM_STRING = "Edm.String"
    EDM_INT32 = "Edm.Int32"
    EDM_INT64 = "Edm.Int64"
    EDM_DOUBLE = "Edm.Double"
    EDM_BOOLEAN = "Edm.Boolean"
    EDM_DATETIME_OFFSET = "Edm.DateTimeOffset"
    COLLECTION_EDM_STRING = "Collection(Edm.String)"
    COLLECTION_EDM_INT32 = "Collection(Edm.Int32)"
    COLLECTION_EDM_INT64 = "Collection(Edm.Int64)"
    COLLECTION_EDM_DOUBLE = "Collection(Edm.Double)"
    COLLECTION_EDM_DATETIME_OFFSET = "Collection(Edm.DateTimeOffset)"


def map_purview_to_search(purview_type: str) -> AzureSearchDataType:
    """Infer Azure Search field type from Purview dataType string."""
    t = purview_type.lower()
    if any(x in t for x in ("char", "text", "string")):
        return AzureSearchDataType.EDM_STRING
    if any(x in t for x in ("bigint", "int64")):
        return AzureSearchDataType.EDM_INT64
    if any(x in t for x in ("int", "smallint", "tinyint")):
        return AzureSearchDataType.EDM_INT32
    if any(x in t for x in ("float", "double", "real", "decimal", "numeric", "money")):
        return AzureSearchDataType.EDM_DOUBLE
    if any(x in t for x in ("bit", "bool", "boolean")):
        return AzureSearchDataType.EDM_BOOLEAN
    if any(x in t for x in ("date", "time", "datetime")):
        return AzureSearchDataType.EDM_DATETIME_OFFSET
    return AzureSearchDataType.EDM_STRING


async def search_catalog(
    client: PurviewCatalogClient, search_keyword: Optional[str] = None
) -> List[Dict[str, Any]]:
    # Search the Purview catalog for assets
    # https://learn.microsoft.com/en-us/rest/api/purview/datamapdataplane/discovery/query?view=rest-purview-datamapdataplane-2023-09-01&tabs=HTTP
    body_input = {"limit": 100}
    if search_keyword:
        body_input["keyword"] = search_keyword
    else:
        body_input["keyword"] = None
    response = client.discovery.query(search_request=body_input)
    return response


async def list_data_sources(client: PurviewScanningClient) -> List[Any]:
    # List all data sources in the Purview account
    # https://learn.microsoft.com/en-us/rest/api/purview/scanningdataplane/data-sources/list?view=rest-purview-scanningdataplane-2023-09-01&tabs=HTTP
    try:
        response = client.data_sources.list_all()
        data_sources = [ds for ds in response]
        return data_sources
    except HttpResponseError as e:
        logger.error(f"Error listing data sources: {e}")
        return []


async def get_assets_by_collection_name(
    client: PurviewCatalogClient, collection_name: str
) -> List[Dict[str, Any]]:
    """Get assets by collection name from Purview Catalog."""
    # https://learn.microsoft.com/en-us/rest/api/purview/datamapdataplane/discovery/query?view=rest-purview-datamapdataplane-2023-09-01&tabs=HTTP#discovery_query_collection
    try:
        response = client.discovery.query(
            search_request={
                "keywords": "*",
                "limit": 100,
                "filter": {"collectionId": collection_name},
            }
        )
        return response.get("value", [])
    except HttpResponseError as e:
        logger.error(f"Error getting assets by collection name: {e}")
        return []


def get_credentials():
    credentials = ClientSecretCredential(
        client_id=client_id, client_secret=client_secret, tenant_id=tenant_id
    )
    return credentials


def get_purview_account_client():
    credentials = get_credentials()
    client = PurviewAccountClient(
        endpoint=purview_account_endpoint, credential=credentials, logging_enable=True
    )
    return client


def get_purview_scan_client():
    credentials = get_credentials()
    client = PurviewScanningClient(
        endpoint=purview_scan_endpoint, credential=credentials, logging_enable=True
    )
    return client


def get_catalog_client():
    credentials = get_credentials()
    client = PurviewCatalogClient(
        endpoint=purview_endpoint, credential=credentials, logging_enable=True
    )
    return client


def get_data_map_client():
    # To use DefaultAzureCredential, ensure environment variables are set
    # AZURE_CLIENT_ID, AZURE_TENANT_ID, AZURE_CLIENT_SECRET
    client = DataMapClient(
        endpoint=purview_endpoint, credential=DefaultAzureCredential()
    )
    return client


def get_admin_client():
    credentials = get_credentials()
    client = PurviewAccountClient(
        endpoint=purview_endpoint, credential=credentials, logging_enable=True
    )
    return client


def get_purview_entity_get(client: DataMapClient, guid: str) -> Dict[str, Any]:
    """Get entity by GUID from Purview Data Map."""
    try:
        response = client.entity.get(guid=guid)
        return response
    except HttpResponseError as e:
        logger.error(f"Error getting entity: {e}")
        return {}


def get_assets_by_purview_data_map_client(client: DataMapClient) -> Any:
    """Get assets by GUID from Purview Data Map."""
    try:
        response = client.discovery.query(body={"limit": 100})
        return response
    except HttpResponseError as e:
        logger.error(f"Error getting assets by GUID: {e}")
        return []


def list_account_collection(client: PurviewAccountClient) -> List[Dict[str, Any]]:
    """List all collections in the Purview account."""
    try:
        response = client.collections.list_collections()
        return [collection for collection in response]
    except HttpResponseError as e:
        logger.error(f"Error listing collections: {e}")
        return []


def search_data_assets(collection_name: str, search_term: str):
    # The Atlas API is a RESTful interface provided by Microsoft Purview, based on Apache Atlas, to programmatically interact with metadata in the Purview Data Map. 
    # 
    url = f"https://{purview_acocunt_name}.purview.azure.com/catalog/api/atlas/v2/search/advanced"
    token_credential = get_credentials().get_token('https://purview.azure.net/.default').token
    headers = {
        "Authorization": f"Bearer {token_credential}",
        "Content-Type": "application/json",
    }

    # Define the search body
    body = {
        "keywords": search_term,  # Search term or keywords
        "limit": 10,  # Limit the number of results
        "filter": {
            "collectionId": collection_name,  # Filter by collection name
        },
    }

    response = requests.post(url, json=body, headers=headers)
    if response.status_code == 200:
        data = response.json()
        search_count = data.get("@search.count", 0)
        print(f"Search count: {search_count}")
        assets = data.get("value", [])
        if assets:
            for asset in assets:
                print(f"Name: {asset.get('name')}, Type: {asset.get('entityType')}, GUID: {asset.get('id')}, qualifiedName: {asset.get('qualifiedName')}")
        else:
            print("No assets found.")
    else:
        print(f"Failed to search data assets: {response.text}")


async def test_purview_api():
    """Test Purview API connectivity and functionality."""
    # Official Clients was not able to fetch assets, so using the REST API (Atlas) directly
    try:
        client_catalog = get_catalog_client()
        search_keyword = "db_scan"
        response_json = await search_catalog(
            client_catalog, search_keyword=search_keyword
        )
        print(response_json)
        # {'@search.count': 0, 'value': [], '@search.facets': None}
        search_count = response_json.get("@search.count")
        print(f"Search count: {search_count}")

        data_sources_list = await list_data_sources(get_purview_scan_client())
        print(f"Data Sources: {data_sources_list}")
        data_source_names = [ds["name"] for ds in data_sources_list]
        print(f"Data Source Names: {data_source_names}")

        guid = "<your-guid-here>"  # Replace with a valid GUID
        entity = get_purview_entity_get(get_data_map_client(), guid)
        print(f"Entity: {entity}")
        # attrs = dir(response_json)

        response = get_assets_by_purview_data_map_client(get_data_map_client())
        print(f"Assets from Data Map Client: {response}")
        # Assets from Data Map Client: {'@search.count': 0, 'value': [], '@search.facets': None, '@search.count.approximate': False}
    
        # assets = await get_assets_by_collection_name(
        #         get_catalog_client(), collection_name
        # )
    except AzureError as e:
        logger.error(f"Purview API error: {e}")


async def main():
    try:
        # https://learn.microsoft.com/en-us/rest/api/purview/accountdataplane/collections/list-collections?view=rest-purview-accountdataplane-2019-11-01-preview&tabs=HTTP
        client = get_purview_account_client()
        collections = list_account_collection(client)
        print(f"Collections: {collections}")
        collection_names = [collection["name"] for collection in collections]
        print(f"Collection Names: {collection_names}")

        collection_name = collection_names[0] if collection_names else None
        if collection_name:
            search_data_assets(collection_name, "*") # Search all assets in the collection
    except AzureError as e:
        logger.error(f"Purview API error: {e}")


if __name__ == "__main__":
    asyncio.run(main())
