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
    original_file_name = data["name"]
    
    target_table = os.environ.get('DESTINATION_TABLE')
    processed_bucket_name = os.environ.get('PROCESSED_BUCKET')
    error_bucket_name = os.environ.get('ERROR_BUCKET')

    # 1. SANITIZE FILENAME 
    # Sanitization is highly recommended to avoid URI encoding issues
    sanitized_file_name = original_file_name.replace("[", "_").replace("]", "_").replace(",", "_").replace(" ", "_")
    
    src_bucket = storage_client.bucket(src_bucket_name)
    current_blob = src_bucket.blob(original_file_name)

    # 2. RENAME LOGIC (Fixed and active)
    if original_file_name != sanitized_file_name:
        print(f"Renaming '{original_file_name}' to '{sanitized_file_name}'")
        # rename_blob handles the copy and delete internally
        current_blob = src_bucket.rename_blob(current_blob, sanitized_file_name)
        active_file_name = sanitized_file_name
    else:
        active_file_name = original_file_name

    # 3. CONFIGURE URI (Must use active_file_name)
    uri = f"gs://{src_bucket_name}/{active_file_name}"
    
    # 4. SCHEMA ENFORCEMENT
    job_schema = [
        bigquery.SchemaField("trade_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("version", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("maturity_date", "DATE", mode="NULLABLE"),
        bigquery.SchemaField("source_timestamp", "TIMESTAMP", mode="NULLABLE"),
        bigquery.SchemaField("reject_reason", "STRING", mode="NULLABLE"),
    ]

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        autodetect=False,
        schema=job_schema,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
    )

    try:
        print(f"Processing {active_file_name} from {src_bucket_name}")
        load_job = bq_client.load_table_from_uri(uri, target_table, job_config=job_config)
        
        # Synchronous wait for completion
        load_job.result() 
        
        print(f"Success. Moving {active_file_name} to {processed_bucket_name}")
        move_file(src_bucket_name, active_file_name, processed_bucket_name)

    except Exception as e:
        print(f"FAILED: {active_file_name}. Error: {str(e)}")
        # Log specific BQ errors if they exist
        if hasattr(e, 'errors'):
            print(f"Detailed Errors: {e.errors}")
            
        move_file(src_bucket_name, active_file_name, error_bucket_name)
        raise e

def move_file(source_bucket_name, file_name, dest_bucket_name):
    if not dest_bucket_name:
        print(f"No destination bucket for {file_name}. Skipping move.")
        return

    source_bucket = storage_client.bucket(source_bucket_name)
    source_blob = source_bucket.blob(file_name)
    dest_bucket = storage_client.bucket(dest_bucket_name)

    # Note: copy_blob does not delete the original; delete() must be called
    source_bucket.copy_blob(source_blob, dest_bucket, file_name)
    source_blob.delete()