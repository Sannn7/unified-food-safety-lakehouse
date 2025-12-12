import os
import json
import requests
from datetime import datetime
from azure.storage.filedatalake import DataLakeServiceClient

# ---------------------------------------------------------
# CONFIGURATION
# ---------------------------------------------------------
CONNECTION_STRING = "KEY_REMOVED_FOR_SECURITY"

# Chicago API (Limit to 5000 rows for the project sample)
SOURCE_URL = "https://data.cityofchicago.org/resource/4ijn-s7e5.json?$limit=5000"

def ingest_chicago():
    try:
        print("🚀 STARTING CHICAGO INGESTION...")
        
        # 1. EXTRACT (Get data from API)
        print(f"   -> Fetching data from City of Chicago API...")
        response = requests.get(SOURCE_URL)
        if response.status_code != 200:
            raise Exception(f"API Failed: {response.status_code}")
        
        data = response.json()
        print(f"   -> Downloaded {len(data)} records.")

        # 2. TRANSFORM (Just adding partitioning)
        # Folder structure: bronze/chicago/2025-12-09/
        today = datetime.now().strftime("%Y-%m-%d")
        file_name = f"chicago/{today}/raw_inspections.json"
        json_data = json.dumps(data)

        # 3. LOAD (Upload to Azure)
        print(f"   -> Connecting to Azure Data Lake...")
        service_client = DataLakeServiceClient.from_connection_string(CONNECTION_STRING)
        file_system_client = service_client.get_file_system_client(file_system="datalake")
        directory_client = file_system_client.get_directory_client("bronze")
        
        print(f"   -> Uploading to 'bronze/{file_name}'...")
        file_client = directory_client.create_file(file_name)
        file_client.append_data(data=json_data, offset=0, length=len(json_data))
        file_client.flush_data(len(json_data))
        
        print("SUCCESS: Chicago data uploaded!")

    except Exception as e:
        print(f"ERROR: {e}")

if __name__ == "__main__":
    ingest_chicago()