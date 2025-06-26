
# Purview-Driven Azure AI Search Type hint

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

7. References: Purview
    - [Purview API Explorer](https://github.com/microsoft/purview-api-samples/): GUI Application to learn how to call the Purview APIs
    - [Purview chat](https://github.com/johnea-chva/purview-chat):  Demo Application integrate with Purview
    - [📺Youtube](https://www.youtube.com/watch?v=Ta-nrefqdb0): Secure your AI apps with user-context-aware controls | Microsoft Purview SDK
    - [PyApacheAtlas: A Python SDK for Azure Purview and Apache Atlas](https://github.com/wjohnson/pyapacheatlas)
    - [Rest API Documentation](https://learn.microsoft.com/en-us/rest/api/purview)
        - [get shcema](https://learn.microsoft.com/en-us/rest/api/purview/datamapdataplane/entity/get)
        - [list collection](https://learn.microsoft.com/en-us/rest/api/purview/accountdataplane/collections/list-collections)

### 🔎 Azure AI Search

1. Create Index by Python SDK

    - [Azure AI Search client library for Python](https://learn.microsoft.com/en-us/python/api/overview/azure/search-documents-readme?view=azure-python)

2. Understand supported data types and how to define an index. 

    - [Supported data types (Azure AI Search)](https://learn.microsoft.com/en-us/rest/api/searchservice/supported-data-types)
    - [Create an index in Azure AI Search](https://learn.microsoft.com/en-us/azure/search/search-how-to-create-search-index?source=recommendations&tabs=portal)
    - [Quickstart: Create a search index in the Azure portal](https://learn.microsoft.com/en-us/azure/search/search-get-started-portal)

3. References: Azure AI Search
    - REST API Definitions & Reponse examples > See this under `aisearch` folder of the project workspace. 
    - [Azure Search Python sample code](https://github.com/Azure-Samples/azure-search-python-samples)

## Azure AI Search Skillset Mapping — Python Analogy

- Skill inputs:
```
name: expected parameter name (like function arg)
source: value from document (e.g. /document/field)
```

- Skill outputs:
```
name: predefined output label
targetName: key used to store output in document (dict key)
```

- Azure AI Search Skillset Json vs Python analogy

```json
"inputs": [
  { "name": "text", "source": "/document/extracted_content" }
],
"outputs": [
  { "name": "textItems", "targetName": "pages" }
]
```

```python
document = {
    "extracted_content": "Some extracted text."
}

def split_skill(text):
    return {
        "pages": text.split("\n\n")
    }

# Run skill
document.update(split_skill(document["extracted_content"]))
```

- Chaining explanation

Each skill’s outputs (targetNames) become keys in the document dict.
Subsequent skills use these keys as source inputs, enabling step-by-step data flow:

```python

'''
{
  "skills": [
    {
      "@odata.type": "#Microsoft.Skills.Util.DocumentExtractionSkill",
      "name": "extraction-skill",
      "context": "/document",
      "inputs": [
        { "name": "file_data", "source": "/document/file_data" }
      ],
      "outputs": [
        { "name": "content", "targetName": "extracted_content" },
        { "name": "normalized_images", "targetName": "normalized_images" }
      ]
    },
    {
      "@odata.type": "#Microsoft.Skills.Text.SplitSkill",
      "name": "split-skill",
      "context": "/document",
      "inputs": [
        { "name": "text", "source": "/document/extracted_content" }
      ],
      "outputs": [
        { "name": "textItems", "targetName": "pages" }
      ]
    }
  ]
}
'''

# Initial document with binary file data
document = {
    "file_data": "binary file content"
}

# Extraction Skill
def extract(file_data):
    return {
        "extracted_content": "Text",
        "normalized_images": ["img1", "img2"]
    }

# Split Skill
def split(text):
    return {
        "pages": text.split("\n\n")
    }

# Run skills in sequence, aligned with Azure JSON mappings
document.update(extract(document["file_data"]))
document.update(split(document["extracted_content"]))

# Final document structure
print(document)
```

- Summary

Azure Term	Python analogy
source	document["key"]
name	function parameter name
targetName	dict key in the output, source references that key for next skill input


## Type hints sample output

```bash
2025-MM-DD 00:01:34.006 | INFO     | __main__:search_data_assets:125 - Search count: **REDACTED**
2025-MM-DD 00:01:34.008 | INFO     | __main__:search_data_assets:130 - Name: Customer, Type: azure_sql_table, GUID: adb80e06-3283-4907-b8f8-7ef6f6f60000, qualifiedName: mssql://**REDACTED**.database.windows.net/**REDACTED**/SalesLT/Customer
2025-MM-DD 00:01:34.009 | INFO     | __main__:search_data_assets:130 - Name: Address, Type: azure_sql_table, GUID: 09d0c95f-4238-44c8-9c7d-5af6f6f60000, qualifiedName: mssql://**REDACTED**.database.windows.net/**REDACTED**/SalesLT/Address
...
2025-MM-DD 00:01:35.393 | INFO     | __main__:main:200 - Table azure_blob_path (**REDACTED**): {}
2025-MM-DD 00:01:35.881 | INFO     | __main__:main:200 - Table azure_sql_table (**REDACTED**): {'ProductCategoryID': <AzureSearchDataType.EDM_INT32: 'Edm.Int32'>, 'Weight': <AzureSearchDataType.EDM_DOUBLE: 'Edm.Double'>, 'ProductID': <AzureSearchDataType.EDM_INT32: 'Edm.Int32'>, 'SellStartDate': <AzureSearchDataType.EDM_STRING: 'Edm.String'>, 'DiscontinuedDate': <AzureSearchDataType.EDM_STRING: 'Edm.String'>, 'ListPrice': <AzureSearchDataType.EDM_DOUBLE: 'Edm.Double'>, 'ThumbNailPhoto': <AzureSearchDataType.EDM_STRING: 'Edm.String'>, 'Name': <AzureSearchDataType.EDM_STRING: 'Edm.String'>, 'SellEndDate': <AzureSearchDataType.EDM_STRING: 'Edm.String'>, 'rowguid': <AzureSearchDataType.EDM_STRING: 'Edm.String'>, 'Size': <AzureSearchDataType.EDM_STRING: 'Edm.String'>, 'Color': <AzureSearchDataType.EDM_STRING: 'Edm.String'>, 'ProductModelID': <AzureSearchDataType.EDM_INT32: 'Edm.Int32'>, 'StandardCost': <AzureSearchDataType.EDM_DOUBLE: 'Edm.Double'>, 'ThumbnailPhotoFileName': <AzureSearchDataType.EDM_STRING: 'Edm.String'>, 'ProductNumber': <AzureSearchDataType.EDM_STRING: 'Edm.String'>, 'ModifiedDate': <AzureSearchDataType.EDM_STRING: 'Edm.String'>}
2025-MM-DD 00:01:36.349 | INFO     | __main__:main:200 - Table azure_sql_view (**REDACTED**): {'Copyright': <AzureSearchDataType.EDM_STRING: 'Edm.String'>, 'Material': <AzureSearchDataType.EDM_STRING: 'Edm.String'>, 'Saddle': <AzureSearchDataType.EDM_STRING: 'Edm.String'>, 'Style': <AzureSearchDataType.EDM_STRING: 'Edm.String'>, 'NoOfYears': <AzureSearchDataType.EDM_STRING: 'Edm.String'>, 'RiderExperience': <AzureSearchDataType.EDM_STRING: 'Edm.String'>, 'MaintenanceDescription': <AzureSearchDataType.EDM_STRING: 'Edm.String'>, 'ProductLine': <AzureSearchDataType.EDM_STRING: 'Edm.String'>, 'ProductURL': <AzureSearchDataType.EDM_STRING: 'Edm.String'>, 'PictureSize': <AzureSearchDataType.EDM_STRING: 'Edm.String'>, 'WarrantyPeriod': <AzureSearchDataType.EDM_STRING: 'Edm.String'>, 'ProductModelID': <AzureSearchDataType.EDM_INT32: 'Edm.Int32'>, 'Pedal': <AzureSearchDataType.EDM_STRING: 'Edm.String'>, 'BikeFrame': <AzureSearchDataType.EDM_STRING: 'Edm.String'>, 'Crankset': <AzureSearchDataType.EDM_STRING: 'Edm.String'>, 'rowguid': <AzureSearchDataType.EDM_STRING: 'Edm.String'>, 'Color': <AzureSearchDataType.EDM_STRING: 'Edm.String'>, 'PictureAngle': <AzureSearchDataType.EDM_STRING: 'Edm.String'>, 'Manufacturer': <AzureSearchDataType.EDM_STRING: 'Edm.String'>, 'Name': <AzureSearchDataType.EDM_STRING: 'Edm.String'>, 'WarrantyDescription': <AzureSearchDataType.EDM_STRING: 'Edm.String'>, 'Wheel': <AzureSearchDataType.EDM_STRING: 'Edm.String'>, 'ModifiedDate': <AzureSearchDataType.EDM_STRING: 'Edm.String'>, 'Summary': <AzureSearchDataType.EDM_STRING: 'Edm.String'>, 'ProductPhotoID': <AzureSearchDataType.EDM_STRING: 'Edm.String'>}
```
