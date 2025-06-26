# Azure AI Search Index Creation Using REST API

The API calls should be executed in the following order.  
This uses the [Tutorial: Index mixed content using multimodal embeddings and the Document Extraction skill](https://learn.microsoft.com/en-us/azure/search/tutorial-document-extraction-multimodal-embeddings) as a reference.

The file with the `.rest` extension can be executed using the [REST Client](https://marketplace.visualstudio.com/items?itemName=humao.rest-client) extension for Visual Studio Code.

Environment variables should be defined under `.vscode > settings.json`.

### settings.json

```json
{
    "rest-client.environmentVariables": {
        "dev": {
            "baseUrl": "", 
            "apiKey": "***REDACTED***",
            "storageConnection": "DefaultEndpointsProtocol=https;AccountName=***REDACTED***",
            "cognitiveServicesUrl": "", 
            // cognitiveServicesUrl: This should be the URL of your Azure AI Services. 
            // If you set the Azure AI Vision endpoint, the request will be failed.
            "cognitiveServicesKey": "***REDACTED***",
            "modelVersion": "2023-04-15", // Predefined: https://learn.microsoft.com/en-us/azure/search/cognitive-search-skill-vision-vectorize
            "vectorizer": "multimodality-vectorizer", 
            // Any name you want to use. See: https://learn.microsoft.com/en-us/azure/search/vector-search-how-to-configure-vectorizer
            "imageProjectionContainer": "doc-extraction-multimodality-img-container", 
            // Any name you want to use. This value set as the same value in the tutorial.
            "connectionString": "ResourceId=/subscriptions/00000000-0000-0000-0000-00000000/resourceGroups/MY-DEMO-RESOURCE-GROUP/providers/Microsoft.Storage/storageAccounts/MY-DEMO-STORAGE-ACCOUNT/;" 
            // Modify this with your actual values.
        }
    }
}
```

### Execution Order

1. Create a data source → `ai_search_create_datasource.rest`  
2. Create an index → `ai_search_create_index.rest`  
3. Create a skillset → `ai_search_create_skillset.rest`  
4. Create and run an indexer → `ai_search_create_run_indexer.rest`  
5. Query the index → `ai_search_run_query.rest`

### API Response sample

- See the files under the `response` directory with `.http` extension.
