"""
Azure Purview Data Source Access and Type Mapping Script
Retrieves connection information and maps column types to Azure AI Search index data types.
"""

import asyncio
import logging
from typing import Dict, List, Any, Optional, Union, Tuple
from dataclasses import dataclass
from enum import Enum
import pandas as pd
import numpy as np
from datetime import datetime, date
from decimal import Decimal

from azure.identity import DefaultAzureCredential
from azure.purview.catalog import PurviewCatalogClient
from azure.purview.scanning import PurviewScanningClient
from azure.core.exceptions import AzureError

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class AzureSearchDataType(Enum):
    """Azure AI Search supported data types"""
    EDM_STRING = "Edm.String"
    EDM_INT32 = "Edm.Int32"
    EDM_INT64 = "Edm.Int64"
    EDM_DOUBLE = "Edm.Double"
    EDM_BOOLEAN = "Edm.Boolean"
    EDM_DATETIME_OFFSET = "Edm.DateTimeOffset"
    EDM_GEOGRAPHY_POINT = "Edm.GeographyPoint"
    EDM_COMPLEX_TYPE = "Edm.ComplexType"
    COLLECTION_EDM_STRING = "Collection(Edm.String)"
    COLLECTION_EDM_INT32 = "Collection(Edm.Int32)"
    COLLECTION_EDM_INT64 = "Collection(Edm.Int64)"
    COLLECTION_EDM_DOUBLE = "Collection(Edm.Double)"
    COLLECTION_EDM_DATETIME_OFFSET = "Collection(Edm.DateTimeOffset)"
    COLLECTION_EDM_GEOGRAPHY_POINT = "Collection(Edm.GeographyPoint)"
    COLLECTION_EDM_COMPLEX_TYPE = "Collection(Edm.ComplexType)"

@dataclass
class ColumnTypeInfo:
    """Information about a column's inferred type"""
    column_name: str
    python_type: type
    nullable: bool
    is_collection: bool
    azure_search_type: AzureSearchDataType
    sample_values: List[Any]

@dataclass
class DataSourceInfo:
    """Information about a data source from Purview"""
    source_name: str
    connection_string: Optional[str]
    database_name: Optional[str]
    schema_name: Optional[str]
    table_name: Optional[str]
    column_types: List[ColumnTypeInfo]

class PurviewDataSourceAnalyzer:
    """Main class for analyzing Purview data sources and mapping types"""
    
    def __init__(self, purview_account_name: str):
        """
        Initialize the Purview analyzer
        
        Args:
            purview_account_name: Name of the Purview account
        """
        self.purview_account_name = purview_account_name
        self.credential = DefaultAzureCredential()
        
        # Initialize Purview clients
        self.catalog_client = PurviewCatalogClient(
            endpoint=f"https://{purview_account_name}.purview.azure.com",
            credential=self.credential
        )
        
        self.scanning_client = PurviewScanningClient(
            endpoint=f"https://{purview_account_name}.purview.azure.com",
            credential=self.credential
        )
        
        logger.info(f"Initialized Purview clients for account: {purview_account_name}")

    def _map_python_type_to_azure_search(
        self, 
        python_type: type, 
        is_collection: bool = False,
        sample_values: List[Any] = None
    ) -> AzureSearchDataType:
        """
        Map Python types to Azure AI Search data types
        
        Args:
            python_type: The Python type to map
            is_collection: Whether this is a collection/array type
            sample_values: Sample values to help with type inference
            
        Returns:
            Corresponding Azure Search data type
        """
        if sample_values is None:
            sample_values = []
        
        # Handle collections/arrays
        if is_collection:
            if python_type in (str, object):
                return AzureSearchDataType.COLLECTION_EDM_STRING
            elif python_type in (int, np.int32):
                return AzureSearchDataType.COLLECTION_EDM_INT32
            elif python_type in (np.int64,):
                return AzureSearchDataType.COLLECTION_EDM_INT64
            elif python_type in (float, np.float64, np.float32):
                return AzureSearchDataType.COLLECTION_EDM_DOUBLE
            elif python_type in (datetime, date):
                return AzureSearchDataType.COLLECTION_EDM_DATETIME_OFFSET
            else:
                return AzureSearchDataType.COLLECTION_EDM_STRING
        
        # Handle scalar types
        if python_type == str:
            return AzureSearchDataType.EDM_STRING
        elif python_type in (int, np.int32):
            # Check if values fit in Int32 range
            if sample_values:
                max_val = max(sample_values) if sample_values else 0
                min_val = min(sample_values) if sample_values else 0
                if min_val >= -2147483648 and max_val <= 2147483647:
                    return AzureSearchDataType.EDM_INT32
                else:
                    return AzureSearchDataType.EDM_INT64
            return AzureSearchDataType.EDM_INT32
        elif python_type in (np.int64,):
            return AzureSearchDataType.EDM_INT64
        elif python_type in (float, np.float64, np.float32, Decimal):
            return AzureSearchDataType.EDM_DOUBLE
        elif python_type == bool:
            return AzureSearchDataType.EDM_BOOLEAN
        elif python_type in (datetime, date):
            return AzureSearchDataType.EDM_DATETIME_OFFSET
        else:
            # Default to string for unknown types
            return AzureSearchDataType.EDM_STRING

    def _infer_column_types(self, df: pd.DataFrame) -> List[ColumnTypeInfo]:
        """
        Infer column types from a pandas DataFrame
        
        Args:
            df: DataFrame to analyze
            
        Returns:
            List of ColumnTypeInfo objects
        """
        column_types = []
        
        for column in df.columns:
            series = df[column]
            
            # Get non-null values for analysis
            non_null_values = series.dropna()
            sample_values = non_null_values.head(10).tolist()
            
            # Determine if nullable
            nullable = series.isnull().any()
            
            # Determine if collection (check if any values are lists/arrays)
            is_collection = False
            if not non_null_values.empty:
                first_value = non_null_values.iloc[0]
                is_collection = isinstance(first_value, (list, tuple, np.ndarray))
            
            # Infer Python type
            if is_collection and not non_null_values.empty:
                # For collections, infer type from first element of first non-null value
                first_collection = non_null_values.iloc[0]
                if len(first_collection) > 0:
                    python_type = type(first_collection[0])
                else:
                    python_type = str
            else:
                # Use pandas inferred dtype
                dtype = series.dtype
                if pd.api.types.is_string_dtype(dtype):
                    python_type = str
                elif pd.api.types.is_integer_dtype(dtype):
                    if dtype == 'int32':
                        python_type = int
                    else:
                        python_type = np.int64
                elif pd.api.types.is_float_dtype(dtype):
                    python_type = float
                elif pd.api.types.is_bool_dtype(dtype):
                    python_type = bool
                elif pd.api.types.is_datetime64_any_dtype(dtype):
                    python_type = datetime
                else:
                    python_type = str
            
            # Map to Azure Search type
            azure_search_type = self._map_python_type_to_azure_search(
                python_type, is_collection, sample_values
            )
            
            column_info = ColumnTypeInfo(
                column_name=column,
                python_type=python_type,
                nullable=nullable,
                is_collection=is_collection,
                azure_search_type=azure_search_type,
                sample_values=sample_values
            )
            
            column_types.append(column_info)
            
        return column_types

    async def get_data_sources(self) -> List[Dict[str, Any]]:
        """
        Retrieve data sources from Purview
        
        Returns:
            List of data source information
        """
        try:
            # Get all data sources
            data_sources = []
            
            # Use the catalog client to search for data sources
            search_filter = {
                "kind": "DataSource"
            }
            
            response = self.catalog_client.discovery.search_entities(
                search_filter=search_filter,
                limit=100
            )
            
            for entity in response.get("value", []):
                data_source = {
                    "guid": entity.get("guid"),
                    "name": entity.get("displayText"),
                    "type": entity.get("entityType"),
                    "qualified_name": entity.get("qualifiedName")
                }
                data_sources.append(data_source)
                
            logger.info(f"Retrieved {len(data_sources)} data sources from Purview")
            return data_sources
            
        except AzureError as e:
            logger.error(f"Error retrieving data sources: {str(e)}")
            raise

    async def get_table_entities(self, data_source_guid: str) -> List[Dict[str, Any]]:
        """
        Get table entities for a specific data source
        
        Args:
            data_source_guid: GUID of the data source
            
        Returns:
            List of table entities
        """
        try:
            # Search for tables related to this data source
            search_filter = {
                "kind": "Table",
                "filter": {
                    "and": [
                        {
                            "attributeName": "dataSource",
                            "operator": "eq",
                            "attributeValue": data_source_guid
                        }
                    ]
                }
            }
            
            response = self.catalog_client.discovery.search_entities(
                search_filter=search_filter,
                limit=50
            )
            
            tables = []
            for entity in response.get("value", []):
                table_info = {
                    "guid": entity.get("guid"),
                    "name": entity.get("displayText"),
                    "qualified_name": entity.get("qualifiedName"),
                    "type": entity.get("entityType")
                }
                tables.append(table_info)
                
            return tables
            
        except AzureError as e:
            logger.error(f"Error retrieving table entities: {str(e)}")
            return []

    async def analyze_table_schema(self, table_guid: str) -> Optional[pd.DataFrame]:
        """
        Analyze table schema and retrieve sample data from actual data source
        
        Args:
            table_guid: GUID of the table to analyze
            
        Returns:
            DataFrame with actual sample data or None if unable to retrieve
        """
        try:
            # Get table entity details
            entity = self.catalog_client.entity.get_entity_by_guid(table_guid)
            
            # Extract connection information and attributes
            entity_data = entity.get("entity", {})
            attributes = entity_data.get("attributes", {})
            qualified_name = attributes.get("qualifiedName", "")
            
            logger.info(f"Analyzing table: {attributes.get('name', 'Unknown')}")
            logger.info(f"Qualified name: {qualified_name}")
            
            # Get column information from the table entity
            columns_info = []
            relationships = entity_data.get("relationshipAttributes", {})
            
            # Get column entities
            if "columns" in relationships:
                columns = relationships["columns"]
                for column_ref in columns:
                    try:
                        column_entity = self.catalog_client.entity.get_entity_by_guid(column_ref["guid"])
                        column_attrs = column_entity.get("entity", {}).get("attributes", {})
                        
                        column_info = {
                            "name": column_attrs.get("name"),
                            "data_type": column_attrs.get("dataType"),
                            "is_nullable": column_attrs.get("isNullable", True),
                            "max_length": column_attrs.get("maxLength"),
                            "precision": column_attrs.get("precision"),
                            "scale": column_attrs.get("scale")
                        }
                        columns_info.append(column_info)
                        
                    except Exception as col_error:
                        logger.warning(f"Could not retrieve column details: {str(col_error)}")
                        continue
            
            if not columns_info:
                logger.warning("No column information found in Purview metadata")
                return None
            
            # Try to establish connection based on data source type
            data_source_type = self._get_data_source_type(qualified_name)
            connection_info = await self._get_connection_info(table_guid, qualified_name)
            
            if connection_info:
                # Attempt to connect and retrieve actual sample data
                df = await self._retrieve_sample_data(connection_info, qualified_name, columns_info)
                if df is not None:
                    return df
            
            # If unable to connect to actual data source, create DataFrame structure from metadata
            logger.info("Creating DataFrame structure from Purview metadata only")
            return self._create_dataframe_from_metadata(columns_info)
            
        except AzureError as e:
            logger.error(f"Error analyzing table schema: {str(e)}")
            return None

    def _get_data_source_type(self, qualified_name: str) -> Optional[str]:
        """
        Determine data source type from qualified name
        
        Args:
            qualified_name: The qualified name of the table
            
        Returns:
            Data source type string
        """
        if "mssql://" in qualified_name or "sqlserver://" in qualified_name:
            return "sql_server"
        elif "abfss://" in qualified_name or "adls" in qualified_name.lower():
            return "azure_data_lake"
        elif "https://" in qualified_name and "blob" in qualified_name:
            return "azure_blob"
        elif "cosmos" in qualified_name.lower():
            return "cosmos_db"
        else:
            return "unknown"

    async def _get_connection_info(self, table_guid: str, qualified_name: str) -> Optional[Dict[str, Any]]:
        """
        Extract connection information from Purview metadata
        
        Args:
            table_guid: GUID of the table
            qualified_name: Qualified name of the table
            
        Returns:
            Connection information dictionary
        """
        try:
            # Get the data source entity that contains this table
            data_source_type = self._get_data_source_type(qualified_name)
            
            # Parse connection details from qualified name and entity relationships
            connection_info = {
                "type": data_source_type,
                "qualified_name": qualified_name
            }
            
            if data_source_type == "sql_server":
                # Extract server, database, schema, table from qualified name
                # Format: mssql://server/database/schema/table
                parts = qualified_name.replace("mssql://", "").split("/")
                if len(parts) >= 4:
                    connection_info.update({
                        "server": parts[0],
                        "database": parts[1],
                        "schema": parts[2],
                        "table": parts[3]
                    })
                    
            elif data_source_type == "azure_data_lake":
                # Extract storage account, container, path from qualified name
                # Format: abfss://container@account.dfs.core.windows.net/path
                if "abfss://" in qualified_name:
                    url_part = qualified_name.replace("abfss://", "")
                    if "@" in url_part:
                        container_account = url_part.split("/")[0]
                        container = container_account.split("@")[0]
                        account = container_account.split("@")[1].split(".")[0]
                        path = "/".join(url_part.split("/")[1:])
                        
                        connection_info.update({
                            "storage_account": account,
                            "container": container,
                            "path": path
                        })
            
            return connection_info
            
        except Exception as e:
            logger.warning(f"Could not extract connection info: {str(e)}")
            return None

    async def _retrieve_sample_data(
        self, 
        connection_info: Dict[str, Any], 
        qualified_name: str, 
        columns_info: List[Dict[str, Any]]
    ) -> Optional[pd.DataFrame]:
        """
        Retrieve actual sample data from the data source
        
        Args:
            connection_info: Connection information
            qualified_name: Qualified name of the table
            columns_info: Column metadata
            
        Returns:
            DataFrame with sample data or None
        """
        try:
            data_source_type = connection_info.get("type")
            
            if data_source_type == "sql_server":
                return await self._retrieve_sql_server_data(connection_info, columns_info)
            elif data_source_type == "azure_data_lake":
                return await self._retrieve_adls_data(connection_info, columns_info)
            elif data_source_type == "azure_blob":
                return await self._retrieve_blob_data(connection_info, columns_info)
            else:
                logger.warning(f"Unsupported data source type: {data_source_type}")
                return None
                
        except Exception as e:
            logger.error(f"Error retrieving sample data: {str(e)}")
            return None

    async def _retrieve_sql_server_data(
        self, 
        connection_info: Dict[str, Any], 
        columns_info: List[Dict[str, Any]]
    ) -> Optional[pd.DataFrame]:
        """
        Retrieve sample data from SQL Server
        
        Args:
            connection_info: SQL Server connection information
            columns_info: Column metadata
            
        Returns:
            DataFrame with sample data
        """
        try:
            import pyodbc
            
            server = connection_info.get("server")
            database = connection_info.get("database")
            schema = connection_info.get("schema")
            table = connection_info.get("table")
            
            # Use Azure AD authentication
            connection_string = (
                f"Driver={{ODBC Driver 18 for SQL Server}};"
                f"Server=tcp:{server},1433;"
                f"Database={database};"
                f"Authentication=ActiveDirectoryDefault;"
                f"Encrypt=yes;"
                f"TrustServerCertificate=no;"
                f"Connection Timeout=30;"
            )
            
            with pyodbc.connect(connection_string) as conn:
                query = f"SELECT TOP 10 * FROM [{schema}].[{table}]"
                df = pd.read_sql(query, conn)
                logger.info(f"Retrieved {len(df)} rows from SQL Server table")
                return df
                
        except ImportError:
            logger.error("pyodbc not installed. Install with: pip install pyodbc")
            return None
        except Exception as e:
            logger.error(f"Error connecting to SQL Server: {str(e)}")
            return None

    async def _retrieve_adls_data(
        self, 
        connection_info: Dict[str, Any], 
        columns_info: List[Dict[str, Any]]
    ) -> Optional[pd.DataFrame]:
        """
        Retrieve sample data from Azure Data Lake Storage
        
        Args:
            connection_info: ADLS connection information
            columns_info: Column metadata
            
        Returns:
            DataFrame with sample data
        """
        try:
            from azure.storage.filedatalake import DataLakeServiceClient
            
            storage_account = connection_info.get("storage_account")
            container = connection_info.get("container")
            path = connection_info.get("path")
            
            service_client = DataLakeServiceClient(
                account_url=f"https://{storage_account}.dfs.core.windows.net",
                credential=self.credential
            )
            
            file_system_client = service_client.get_file_system_client(file_system=container)
            file_client = file_system_client.get_file_client(path)
            
            # Download file content
            download = file_client.download_file()
            content = download.readall()
            
            # Determine file format and read accordingly
            if path.endswith('.csv'):
                import io
                df = pd.read_csv(io.StringIO(content.decode('utf-8')))
                # Limit to first 10 rows
                df = df.head(10)
                logger.info(f"Retrieved {len(df)} rows from ADLS CSV file")
                return df
            elif path.endswith('.parquet'):
                import io
                df = pd.read_parquet(io.BytesIO(content))
                df = df.head(10)
                logger.info(f"Retrieved {len(df)} rows from ADLS Parquet file")
                return df
            else:
                logger.warning(f"Unsupported file format for path: {path}")
                return None
                
        except ImportError as ie:
            logger.error(f"Required package not installed: {str(ie)}")
            return None
        except Exception as e:
            logger.error(f"Error connecting to ADLS: {str(e)}")
            return None

    async def _retrieve_blob_data(
        self, 
        connection_info: Dict[str, Any], 
        columns_info: List[Dict[str, Any]]
    ) -> Optional[pd.DataFrame]:
        """
        Retrieve sample data from Azure Blob Storage
        
        Args:
            connection_info: Blob storage connection information
            columns_info: Column metadata
            
        Returns:
            DataFrame with sample data
        """
        try:
            from azure.storage.blob import BlobServiceClient
            
            # Extract blob info from qualified name
            qualified_name = connection_info.get("qualified_name", "")
            # Implementation would depend on blob URL format in Purview
            
            logger.warning("Azure Blob Storage data retrieval not fully implemented")
            return None
            
        except ImportError:
            logger.error("azure-storage-blob not installed. Install with: pip install azure-storage-blob")
            return None
        except Exception as e:
            logger.error(f"Error connecting to Blob Storage: {str(e)}")
            return None

    def _create_dataframe_from_metadata(self, columns_info: List[Dict[str, Any]]) -> pd.DataFrame:
        """
        Create an empty DataFrame structure from column metadata
        
        Args:
            columns_info: List of column information from Purview
            
        Returns:
            Empty DataFrame with proper column types
        """
        if not columns_info:
            return pd.DataFrame()
        
        # Create empty DataFrame with column names
        column_names = [col["name"] for col in columns_info if col["name"]]
        df = pd.DataFrame(columns=column_names)
        
        # Set appropriate dtypes based on metadata
        for col_info in columns_info:
            col_name = col_info["name"]
            data_type = col_info.get("data_type", "").lower()
            
            if col_name in df.columns:
                if "int" in data_type or "integer" in data_type:
                    df[col_name] = df[col_name].astype('Int64')  # Nullable integer
                elif "float" in data_type or "double" in data_type or "decimal" in data_type:
                    df[col_name] = df[col_name].astype('float64')
                elif "bool" in data_type or "bit" in data_type:
                    df[col_name] = df[col_name].astype('boolean')
                elif "date" in data_type or "time" in data_type:
                    df[col_name] = pd.to_datetime(df[col_name])
                else:
                    df[col_name] = df[col_name].astype('string')
        
        logger.info(f"Created DataFrame structure with {len(df.columns)} columns from metadata")
        return df

async def main():
    """Main execution function"""
    print('Azure Purview Data Source Access Script')
    
    # Configuration - replace with your Purview account name
    PURVIEW_ACCOUNT_NAME = "your-purview-account-name"
    
    try:
        # Initialize analyzer
        analyzer = PurviewDataSourceAnalyzer(PURVIEW_ACCOUNT_NAME)
        
        # Process data sources
        logger.info("Starting data source analysis...")
        data_sources = await analyzer.process_data_sources()
        
        # Generate results
        logger.info(f"Analyzed {len(data_sources)} data sources")
        
        for source in data_sources:
            print(f"\nData Source: {source.source_name}")
            print(f"Table: {source.table_name}")
            print("Column Type Mappings:")
            
            for column in source.column_types:
                print(f"  {column.column_name}:")
                print(f"    Python Type: {column.python_type.__name__}")
                print(f"    Azure Search Type: {column.azure_search_type.value}")
                print(f"    Nullable: {column.nullable}")
                print(f"    Is Collection: {column.is_collection}")
                print(f"    Sample Values: {column.sample_values[:3]}...")
        
        # Generate Azure Search index schema
        if data_sources:
            schema = analyzer.generate_azure_search_index_schema(data_sources)
            print(f"\nGenerated Azure AI Search Index Schema:")
            print(f"Index Name: {schema['name']}")
            print(f"Total Fields: {len(schema['fields'])}")
            
            # Print first few fields as example
            for field in schema['fields'][:5]:
                print(f"  Field: {field['name']} ({field['type']})")
                
    except Exception as e:
        logger.error(f"Error in main execution: {str(e)}")
        raise

if __name__ == "__main__":
    asyncio.run(main())