
# Purview-Driven Azure AI Search

🧪 Proof of Concept (PoC) script to evaluate the feasibility of the Purview API, aiming to generate type hints and connection details for an Azure AI Search index based on schema data retrieved from the Purview Data Catalog.

## Environment configuration

### 🧭 Micrsoft Purview

1. Create Microsoft Purview account in Azure or via the [Purview Portal](https://purview.microsoft.com/). 

    - [quickstart for creating a Microsoft Purview account](https://learn.microsoft.com/en-us/purview/create-microsoft-purview-portal)

2. Grant `Reader` permission to Managed Identity associated with Purview account

    - [Scanning multiple Azure data sources](https://learn.microsoft.com/en-us/purview/troubleshoot-connections?wt.mc_id=mspurview_inproduct_learnmoreerrorlinks_troubleshootscanconnection_csadai#scanning-data-sources-using-private-link)

3. Create a Storage Account, then upgrade it to Data Lake Storage Gen2 in Azure, and upload sample data to the storage.

    - [Create a storage account to use with Azure Data Lake Storage](https://learn.microsoft.com/en-us/azure/storage/blobs/create-data-lake-storage-account)
    - [Azure AI Search Sample Data](https://github.com/Azure-Samples/azure-search-sample-data)
    - (another option) [Azure Data SQL Samples](https://github.com/microsoft/sql-server-samples) > `samples/databases/northwind-pubs`

4. Open Microsoft Purview Governance Portal > Scan data sources in Data Map

    - [Scan data sources in Data Map](https://learn.microsoft.com/en-us/purview/data-map-scan-data-sources)

5. Create a Service Principal for API access to Purview.

    - [Create a service principal (application)](https://learn.microsoft.com/en-us/purview/data-gov-api-rest-data-plane)

### 🔎 Azure AI Search

1. Create Index by Python SDK

    - [Azure AI Search client library for Python](https://learn.microsoft.com/en-us/python/api/overview/azure/search-documents-readme?view=azure-python)

2. Understand supported data types and how to define an index. 

    - [Supported data types (Azure AI Search)](https://learn.microsoft.com/en-us/rest/api/searchservice/supported-data-types)
    - [Create an index in Azure AI Search](https://learn.microsoft.com/en-us/azure/search/search-how-to-create-search-index?source=recommendations&tabs=portal)
