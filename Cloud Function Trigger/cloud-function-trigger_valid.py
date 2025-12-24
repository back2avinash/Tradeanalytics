import os
import functions_framework
from google.cloud import bigquery
from google.cloud import storage

# Initialize clients globally for connection pooling and better performance
bq_client = bigquery.Client()
storage_client = storage.Client()

@functions_framework.cloud_event
def process_gcs_to_bigquery(cloud_event):
    data = cloud_event.data
    src_bucket_name = data["bucket"]
    original_file_name = data["name"]

    data = cloud_event.data
    
    # --- ADDED: Print all event metadata for debugging ---
    print(f"Full Event Data: {data}")
    
    src_bucket_name = data["bucket"]
    original_file_name = data["name"]
    file_size = data.get("size", "Unknown")
    content_type = data.get("contentType", "Unknown")

    # --- ADDED: Print specific key values ---
    print(f"Processing File: {original_file_name}")
    print(f"From Bucket: {src_bucket_name}")
    print(f"File Size: {file_size} bytes")
    print(f"Content Type: {content_type}")
    
    # Guard Clause to ignore internal folders and BQ staging files ---
    # This prevents the function from triggering itself in an infinite loop
    if (original_file_name.endswith("/") or 
        "beam-temp" in original_file_name or 
        ".tmp" in original_file_name):
        print(f"AVINASH--> Skipping internal staging or folder object: {original_file_name}")
        return    
    
    # Optional: Log file details for valid triggers
    print(f"Processing Valid File: {original_file_name}")            
    target_table = os.environ.get('DESTINATION_TABLE')
    processed_bucket_name = os.environ.get('PROCESSED_BUCKET')
    error_bucket_name = os.environ.get('ERROR_BUCKET')

    # 1. SANITIZE FILENAME
    sanitized_file_name = original_file_name.replace("[", "_").replace("]", "_").replace(",", "_").replace(" ", "_")
    
    src_bucket = storage_client.bucket(src_bucket_name)
    current_blob = src_bucket.blob(original_file_name)

    # 2. RENAME LOGIC
    if original_file_name != sanitized_file_name:
        print(f"Renaming {original_file_name} to {sanitized_file_name}")
        current_blob = src_bucket.rename_blob(current_blob, sanitized_file_name)
        active_file_name = sanitized_file_name
    else:
        active_file_name = original_file_name
    
    # URI must point to the name AFTER renaming
    uri = f"gs://{src_bucket_name}/{active_file_name}"
    
    # 3. SCHEMA DEFINITION
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
        
        # Wait for the job to complete
        load_job.result() 
        
        print(f"Success. Moving {active_file_name} to {processed_bucket_name}")
        # Passing src_bucket_name to handle cleanup inside move_file
        move_file(src_bucket_name, active_file_name, processed_bucket_name)

    except Exception as e:
        print(f"FAILED: {active_file_name}. Error: {str(e)}")
        if 'load_job' in locals() and load_job.errors:
            print(f"BQ Error Details: {load_job.errors}")
            
        move_file(src_bucket_name, active_file_name, error_bucket_name)
        raise e

def move_file(source_bucket_name, file_name, dest_bucket_name):
    if not dest_bucket_name:
        print("No destination bucket provided, skipping move.")
        return
        
    source_bucket = storage_client.bucket(source_bucket_name)
    source_blob = source_bucket.blob(file_name)
    dest_bucket = storage_client.bucket(dest_bucket_name)
    
    # 1. Move the primary data file
    source_bucket.copy_blob(source_blob, dest_bucket, file_name)
    source_blob.delete()

    # Cleanup logic for BigQuery/Beam staging artifacts ---
    # We search for the common prefix pattern BigQuery uses for these temp folders
    # Usually it takes the form: beam-temp-<filename>-<timestamp>
    file_base = file_name.split('.')[0] # e.g., 'tradea' from 'tradea.json'
    temp_prefix = f"beam-temp-{file_base}"
    
    blobs_to_delete = source_bucket.list_blobs(prefix=temp_prefix)
    
    for temp_blob in blobs_to_delete:
        print(f"Cleaning up staging artifact: {temp_blob.name}")
        temp_blob.delete()