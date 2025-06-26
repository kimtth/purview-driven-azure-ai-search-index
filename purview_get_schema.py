import asyncio
import os
from enum import Enum
import sys
from loguru import logger
from typing import List, Dict, Any, Optional

from azure.identity import ClientSecretCredential
from azure.purview.administration.account import PurviewAccountClient
from azure.core.exceptions import AzureError, HttpResponseError

from dotenv import load_dotenv
import requests
import json

load_dotenv(verbose=True)

# Add a console sink
logger.add(sys.stderr, level="DEBUG")

# Add a file sink
logger.add("purview_output.log", level="INFO")


client_id = os.getenv("AZURE_CLIENT_ID")
client_secret = os.getenv("AZURE_CLIENT_SECRET")
tenant_id = os.getenv("AZURE_TENANT_ID")
purview_account_name = os.getenv("PURVIEW_ACCOUNT_NAME")
purview_account_endpoint = os.getenv("PURVIEW_ACCOUNT_ENDPOINT")
# # The Atlas API is a RESTful interface provided by Microsoft Purview, based on Apache Atlas, to programmatically interact with metadata in the Purview Data Map.
atlas_url = f"https://{purview_account_name}.purview.azure.com/catalog/api/atlas/v2"

# Add a single session for connection pooling
session = requests.Session()

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


def map_purview_to_search(purview_type: str | None) -> AzureSearchDataType:
    """Infer Azure Search field type from Purview/Azure SQL Server dataType string."""
    t = (purview_type or "").lower()
    # string types
    if any(x in t for x in ("varchar", "nvarchar", "char", "text", "ntext", "uniqueidentifier")):
        return AzureSearchDataType.EDM_STRING
    # boolean
    if "bit" in t:
        return AzureSearchDataType.EDM_BOOLEAN
    # 32-bit integers
    if any(x in t for x in ("tinyint", "smallint", "int")):
        return AzureSearchDataType.EDM_INT32
    # 64-bit integer
    if "bigint" in t:
        return AzureSearchDataType.EDM_INT64
    # floating-point
    if any(x in t for x in ("float", "real")):
        return AzureSearchDataType.EDM_DOUBLE
    # fixed-precision numeric
    if any(x in t for x in ("decimal", "numeric", "money", "smallmoney")):
        return AzureSearchDataType.EDM_DOUBLE
    # date/time types
    # if any(x in t for x in ("datetimeoffset", "datetime2", "smalldatetime", "date", "time")):
    #     return AzureSearchDataType.EDM_DATETIME_OFFSET
    # fallback to string
    return AzureSearchDataType.EDM_STRING


def get_credentials():
    credentials = ClientSecretCredential(
        client_id=client_id, client_secret=client_secret, tenant_id=tenant_id
    )
    return credentials


def get_atlas_credentials() -> str:
    token_credential = (
        get_credentials().get_token("https://purview.azure.net/.default").token
    )
    return token_credential


def get_purview_account_client():
    credentials = get_credentials()
    client = PurviewAccountClient(
        endpoint=purview_account_endpoint, credential=credentials, logging_enable=True
    )
    return client


def list_account_collection(client: PurviewAccountClient) -> List[Dict[str, Any]]:
    """List all collections in the Purview account."""
    try:
        response = client.collections.list_collections()
        return [collection for collection in response]
    except HttpResponseError as e:
        logger.error(f"Error listing collections: {e}")
        return []


def search_data_assets(collection_name: str, search_term: str, headers: Dict[str, str]) -> List[Dict[str, Any]]:
    """Search for data assets in a specific collection using a shared session & headers."""
    # Define the search body
    body = {
        "keywords": search_term,  # Search term or keywords
        "limit": 10,  # Limit the number of results
        "filter": {
            "collectionId": collection_name,  # Filter by collection name
        },
    }

    response = session.post(f'{atlas_url}/search/advanced', json=body, headers=headers)
    if response.status_code == 200:
        data = response.json()
        search_count = data.get("@search.count", 0)
        logger.info(f"Search count: {search_count}")
        assets = data.get("value", [])

        if assets:
            for asset in assets:
                logger.info(
                    f"Name: {asset.get('name')}, Type: {asset.get('entityType')}, GUID: {asset.get('id')}, qualifiedName: {asset.get('qualifiedName')}"
                )
            return assets
        else:
            logger.info("No assets found.")
    else:
        logger.info(f"Failed to search data assets: {response.text}")


def get_asset_schema(guid: str, headers: Dict[str, str]) -> Dict[str, Any]:
    """Get the schema of a specific asset using a shared session & headers."""
    response = session.get(
        f"{atlas_url}/entity/guid/{guid}",
        headers=headers,
    )
    if response.status_code == 200:
        return response.json()
    else:
        logger.error(f"Failed to get asset schema: {response.text}")
        return {}


def store_asset_schema(guid: str, schema: Dict[str, Any]) -> None:
    """Store the schema of a specific asset."""
    # store the schema to a JSON file for debugging
    debug_dir = "schema_debug"
    os.makedirs(debug_dir, exist_ok=True)
    file_path = os.path.join(debug_dir, f"{guid}_schema.json")
    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(schema, f, indent=2, ensure_ascii=False)
    logger.info(f"Saved schema for GUID {guid} to {file_path}")

async def main():
    try:
        client = get_purview_account_client()
        collections = list_account_collection(client)
        collection_names = [collection["name"] for collection in collections]
        print(f"Collection Names: {collection_names}")

        collection_name = collection_names[0] if collection_names else None
        if collection_name:
            # Fetch and cache Atlas token & headers once
            atlas_token = get_atlas_credentials()
            headers = {
                "Authorization": f"Bearer {atlas_token}",
                "Content-Type": "application/json",
            }

            assets = search_data_assets(
                collection_name, "*", headers
            )  # Search all assets in the collection
            for asset in assets:
                guid = asset.get("id")
                if not guid:
                    logger.warning("No GUID found for asset.")
                    continue

                schema = get_asset_schema(guid, headers)
                # store_asset_schema(guid, schema) # Debugging

                # extract column names and data types from referredEntities
                search_fields: Dict[str, AzureSearchDataType] = {}
                for col in schema.get("referredEntities", {}).values():
                    attrs = col.get("attributes", {})
                    name = attrs.get("name")
                    dtype = attrs.get("data_type")
                    if name and dtype:
                        search_fields[name] = map_purview_to_search(dtype)
                table = schema.get("entity", {})
                logger.info(f"Table {table.get('typeName')} ({guid}): {search_fields}")

    except AzureError as e:
        logger.error(f"Purview API error: {e}")


if __name__ == "__main__":
    asyncio.run(main())
