import os
# from azure.storage.file.datalake import DataLakeServiceClient
from azure.storage.filedatalake import DataLakeServiceClient

# ---------------------------------------------------------
# 1. AUTHENTICATION
# This string acts as your username and password combined.
# ---------------------------------------------------------
connection_string = "KEY_REMOVED_FOR_SECURITY"

def verify_connection():
    try:
        print("Connecting to Azure...")
        
        # 2. INITIALIZE CLIENT
        service_client = DataLakeServiceClient.from_connection_string(connection_string)
        
        # 3. CONNECT TO THE 'datalake' CONTAINER
        file_system_client = service_client.get_file_system_client(file_system="datalake")
        
        # 4. LIST CONTENTS
        paths = file_system_client.get_paths()
        
        print("\n✅ SUCCESS: Connected to Azure Data Lake!")
        print("📂 Folders found:")
        
        # 5. PRINT FOLDER NAMES
        count = 0
        for path in paths:
            print(f" - {path.name}")
            count += 1
            
        if count == 0:
            print("   (No folders found. Did you create bronze/silver/gold?)")
            
    except Exception as e:
        print(f"\n❌ ERROR: Could not connect.")
        print(f"Details: {e}")

if __name__ == "__main__":
    verify_connection()