
# Purview-Driven Azure AI Search

🧪 Proof of Concept (PoC) script to evaluate the feasibility of the Purview API, aiming to generate type hints and connection details for an Azure AI Search index based on schema data retrieved from the Purview Data Catalog.

## Environment configuration

### 🧭 Micrsoft Purview

1. Create Microsoft Purview account in Azure or via the [Purview Portal](https://purview.microsoft.com/). 

    - [quickstart for creating a Microsoft Purview account](https://learn.microsoft.com/en-us/purview/create-microsoft-purview-portal)

2. Grant `Reader` permission to Managed Identity associated with Purview account

    - [Scanning multiple Azure data sources](https://learn.microsoft.com/en-us/purview/troubleshoot-connections?wt.mc_id=mspurview_inproduct_learnmoreerrorlinks_troubleshootscanconnection_csadai#scanning-data-sources-using-private-link)

3. Create data sources and sample data

    - Create a Storage Account, then upgrade it to Data Lake Storage Gen2 in Azure, and upload sample data to the storage. 
        - [Create a storage account to use with Azure Data Lake Storage](https://learn.microsoft.com/en-us/azure/storage/blobs/create-data-lake-storage-account)
        - [Azure AI Search Sample Data](https://github.com/Azure-Samples/azure-search-sample-data) > `hotelreviews`
    - Create an Azure SQL Server to test the connection to Purview.
        - [Azure Data SQL Samples](https://github.com/microsoft/sql-server-samples) > `samples/databases/northwind-pubs`
        - [Quickstart: Use SSMS to connect to and query Azure SQL Database or Azure SQL Managed Instance](https://learn.microsoft.com/en-us/azure/azure-sql/database/connect-query-ssms?view=azuresql)

4. Open Microsoft Purview Governance Portal > Scan data sources in Data Map

    - [Scan data sources in Data Map](https://learn.microsoft.com/en-us/purview/data-map-scan-data-sources)
    - [Discover and govern Azure SQL Database in Microsoft Purview](https://learn.microsoft.com/en-us/purview/register-scan-azure-sql-database?tabs=sql-authentication): SQL authentication.

5. Create a Service Principal for API access to Purview.

    - [Create a service principal (application)](https://learn.microsoft.com/en-us/purview/data-gov-api-rest-data-plane)

6. Use the Microsoft Purview Python SDK
    - [Tutorial: Use the Microsoft Purview Python SDK](https://learn.microsoft.com/en-us/purview/data-gov-python-sdk)

### 🔎 Azure AI Search

1. Create Index by Python SDK

    - [Azure AI Search client library for Python](https://learn.microsoft.com/en-us/python/api/overview/azure/search-documents-readme?view=azure-python)

2. Understand supported data types and how to define an index. 

    - [Supported data types (Azure AI Search)](https://learn.microsoft.com/en-us/rest/api/searchservice/supported-data-types)
    - [Create an index in Azure AI Search](https://learn.microsoft.com/en-us/azure/search/search-how-to-create-search-index?source=recommendations&tabs=portal)
    - [Quickstart: Create a search index in the Azure portal](https://learn.microsoft.com/en-us/azure/search/search-get-started-portal)

