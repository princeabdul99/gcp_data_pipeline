import sys
from google.cloud import storage
from pathlib import Path
from google.api_core.exceptions import GoogleAPIError


PROJECT_ID = "<your-project_id>"
BUCKET_NAME = "<your-bucket-name>"


def load_multiple_data_to_gcs(source_dir:str, folder_name:str):
    try:
        source_path = Path(source_dir)
        if not source_path.exists():
            raise FileNotFoundError(f"Directory not found: {source_path}")
        
        csv_files = list(source_path.glob("*.csv"))

        if not csv_files:
            raise FileNotFoundError(f"No CSV files found in directory: {source_path}")
        

        client = storage.Client(project=PROJECT_ID)
        bucket = client.get_bucket(BUCKET_NAME)

        print(f"Connected to Bucket {bucket.name}")
        print(f"Found {len(csv_files)} CSV file(s) to upload.")
        
        for file_path in csv_files:
            try:
                blob_path = f"{folder_name.rstrip('/')}/{file_path.name}"
                blob = bucket.blob(blob_path)

                blob.upload_from_filename(file_path)

                print(
                    f"Uploaded {file_path} "
                    f"to gs://{BUCKET_NAME}/{blob_path}"
                ) 
            except GoogleAPIError as e:
                print(f"GCS ERROR Uploading {file_path}: {e}", file=sys.stderr)  
                raise


            except Exception as e:
                print(f"UNEXPECTED ERROR Uploading {file_path}: {e}", file=sys.stderr)  
                raise               

    except FileNotFoundError as e:
        print(f"FILE SYSYTEM ERROR: {e}", file=sys.stderr)  
        raise

    except GoogleAPIError as e:
        print(f"GCS ERROR: {e}", file=sys.stderr)  
        raise


    except Exception as e:
        print(f"UNEXPECTED ERROR: {e}", file=sys.stderr)  
        raise


if __name__ == '__main__':
    load_multiple_data_to_gcs(
        source_dir="/data/customers",
        folder_name="customers"    
    )

