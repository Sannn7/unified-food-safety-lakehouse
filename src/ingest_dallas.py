import os
import json
import requests
from datetime import datetime
from azure.storage.filedatalake import DataLakeServiceClient

# ---------------------------------------------------------
# CONFIGURATION
# ---------------------------------------------------------
CONNECTION_STRING = "KEY_REMOVED_FOR_SECURITY"

# Dallas API (Limit to 5000 rows)
SOURCE_URL = "https://www.dallasopendata.com/resource/dri5-wcct.json?$limit=5000"

def ingest_dallas():
    try:
        print("STARTING DALLAS INGESTION...")
        
        # 1. EXTRACT
        print(f"   -> Fetching data from City of Dallas API...")
        response = requests.get(SOURCE_URL)
        if response.status_code != 200:
            raise Exception(f"API Failed: {response.status_code}")
            
        data = response.json()
        print(f"   -> Downloaded {len(data)} records.")

        # 2. TRANSFORM
        # Folder structure: bronze/dallas/2025-12-09/
        today = datetime.now().strftime("%Y-%m-%d")
        file_name = f"dallas/{today}/raw_inspections.json"
        json_data = json.dumps(data)

        # 3. LOAD
        print(f"   -> Connecting to Azure Data Lake...")
        service_client = DataLakeServiceClient.from_connection_string(CONNECTION_STRING)
        file_system_client = service_client.get_file_system_client(file_system="datalake")
        directory_client = file_system_client.get_directory_client("bronze")
        
        print(f"   -> Uploading to 'bronze/{file_name}'...")
        file_client = directory_client.create_file(file_name)
        file_client.append_data(data=json_data, offset=0, length=len(json_data))
        file_client.flush_data(len(json_data))
        
        print("SUCCESS: Dallas data uploaded!")

    except Exception as e:
        print(f"ERROR: {e}")

if __name__ == "__main__":
    ingest_dallas()