import os
import functions_framework
from google.cloud import bigquery
from google.cloud import storage

# Initialize clients globally
bq_client = bigquery.Client()
storage_client = storage.Client()

@functions_framework.cloud_event
def process_gcs_to_bigquery(cloud_event):
    data = cloud_event.data
    src_bucket_name = data["bucket"]
    source_file_name = data["name"]

    print(f"Full Event Data: {data}")

    # --- PARSE CONFIGURATION ---
    archive_config = os.environ.get('ARCHIVE_BUCKET', '')
    error_config = os.environ.get('ERROR_BUCKET', '')

    archive_bucket, archive_folder = parse_bucket_path(archive_config)
    error_bucket, error_folder = parse_bucket_path(error_config)

    target_table = os.environ.get('DESTINATION_TABLE')
    uri = f"gs://{src_bucket_name}/{source_file_name}"

    # SCHEMA DEFINITION
    job_schema = [
        bigquery.SchemaField("trade_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("version", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("maturity_date", "DATE", mode="REQUIRED"),
        bigquery.SchemaField("timestamp", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("status", "STRING", mode="REQUIRED")
    ]

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        autodetect=False,
        schema=job_schema,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
    )

    try:
        print(f"Starting BQ Load: {uri} -> {target_table}")
        load_job = bq_client.load_table_from_uri(uri, target_table, job_config=job_config)
        load_job.result() # Wait for completion

        print(f"Success. Moving {source_file_name} to {archive_bucket}/{archive_folder}")

        # --- SUCCESS MOVE ---
        move_file(src_bucket_name, source_file_name, archive_bucket, archive_folder)

    except Exception as e:
        print(f"FAILED: {source_file_name}. Error: {str(e)}")
        if 'load_job' in locals() and load_job.errors:
            print(f"BQ Error Details: {load_job.errors}")

        # --- ERROR MOVE ---
        print("Moving to Error Bucket due to failure...")
        move_file(src_bucket_name, source_file_name, error_bucket, error_folder)

        # Raise exception to ensure the function is marked as failed in logs
        raise e

def parse_bucket_path(full_path):
    if not full_path:
        return None, None
    full_path = full_path.replace("gs://", "")
    parts = full_path.split('/', 1)
    bucket = parts[0]
    folder = parts[1] if len(parts) > 1 else ""
    if folder:
        folder = folder.rstrip('/')
    return bucket, folder

def move_file(source_bucket_name, file_name, dest_bucket_name, folder_name=None):
    """
    Copies a file to destination and explicitly deletes the source.
    """
    if not dest_bucket_name:
        print("No destination bucket provided, skipping move.")
        return

    source_bucket = storage_client.bucket(source_bucket_name)
    source_blob = source_bucket.blob(file_name)
    dest_bucket = storage_client.bucket(dest_bucket_name)

    # Prepare destination name
    clean_file_name = os.path.basename(file_name)
    if folder_name:
        destination_blob_name = f"{folder_name}/{clean_file_name}"
    else:
        destination_blob_name = clean_file_name

    print(f"Copying: gs://{source_bucket_name}/{file_name} -> gs://{dest_bucket_name}/{destination_blob_name}")

    try:
        # 1. COPY
        source_bucket.copy_blob(source_blob, dest_bucket, destination_blob_name)
        print("Copy successful.")

        # 2. DELETE (with explicit error handling)
        try:
            source_bucket.delete_blob(file_name)
            print(f"Deleted original file: {file_name}")
        except Exception as delete_error:
            # Check permissions if this prints!
            print(f"CRITICAL WARNING: File copied but DELETE failed. Check IAM Permissions. Error: {delete_error}")

    except Exception as copy_error:
        print(f"Copy failed. File remains in source. Error: {copy_error}")
        raise copy_error