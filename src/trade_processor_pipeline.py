import apache_beam as beam
import apache_beam.transforms.window as window
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions, GoogleCloudOptions, SetupOptions
from apache_beam.io.gcp.bigquery import WriteToBigQuery, BigQueryDisposition, RetryStrategy
from apache_beam.io import WriteToText
from apache_beam.transforms.userstate import ReadModifyWriteStateSpec
from apache_beam.coders import VarIntCoder
from google.cloud import storage
from pydantic import BaseModel, ValidationError, field_validator
from datetime import datetime, date
import argparse
import json
import logging
import uuid

# ==========================================
# 1. Schema Loader
# ==========================================
def load_schema_from_gcs(gcs_path, project_id):
    """
    Loads a single schema JSON file (List of fields) from GCS.
    """
    try:
        if not gcs_path.startswith("gs://"):
            raise ValueError(f"Schema path must start with gs://. Got: {gcs_path}")

        bucket_name = gcs_path.replace("gs://", "").split("/")[0]
        blob_name = "/".join(gcs_path.replace("gs://", "").split("/")[1:])

        storage_client = storage.Client(project=project_id)
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(blob_name)
        content = blob.download_as_text()
        logging.info(f"Loaded schema from {gcs_path}")
        return json.loads(content)
    except Exception as e:
        logging.error(f"Failed to load schema from {gcs_path}: {e}")
        raise

# ==========================================
# 2. Pydantic Model
# ==========================================
class TradeRecord(BaseModel):
    trade_id: str
    version: int
    status: str | None = "NEW"
    trade_type: str | None = None
    instrument_type: str | None = None
    symbol: str | None = None
    amount: float | None = 0.0
    price: float | None = 0.0
    ccy_pair: str | None = None
    counterparty: str | None = None
    trader_id: str | None = None
    execution_venue: str | None = None
    maturity_date: str | None = None
    trade_date: str | None = None
    timestamp: str
    is_historical_load: bool = False

    @field_validator('version', mode='before')
    def parse_version(cls, v):
        if v is None: return 1
        return int(v)

    @field_validator('amount', 'price', mode='before')
    def validate_numbers(cls, v):
        try:
            return float(v) if v is not None else 0.0
        except:
            return 0.0

    @field_validator('maturity_date')
    def validate_date_format(cls, v):
        if v:
            try:
                datetime.strptime(v, '%Y-%m-%d')
            except ValueError:
                raise ValueError("Incorrect date format, should be YYYY-MM-DD")
        return v

# ==========================================
# 3. Helper DoFns
# ==========================================
class ParseAndKeyTrades(beam.DoFn):
    def process(self, element):
        try:
            record = json.loads(element.decode('utf-8'))
            trade_id = record.get('trade_id')
            if not trade_id:
                trade_id = str(uuid.uuid4())
            yield (trade_id, record)
        except Exception:
            pass

class SplitDuplicates(beam.DoFn):
    def process(self, element):
        trade_id, records_iter = element
        records = list(records_iter)
        records.sort(key=lambda x: x.get('timestamp', ''))

        if records:
            yield beam.pvalue.TaggedOutput('unique', records[0])
            for duplicate in records[1:]:
                duplicate['_dedup_note'] = 'Dropped as duplicate'
                yield beam.pvalue.TaggedOutput('duplicates', duplicate)

# ==========================================
# 4. Stateful Processor (FIXED: Added rejection_id)
# ==========================================
class ProcessTrades(beam.DoFn):

    VERSION_STATE = ReadModifyWriteStateSpec('current_version', VarIntCoder())

    def process(self, element, current_version_state=beam.DoFn.StateParam(VERSION_STATE)):
        # Unpack the tuple
        trade_id, record = element

        try:
            # --- 0. Parse ---
            try:
                trade = TradeRecord(**record)
            except ValidationError as e:
                yield beam.pvalue.TaggedOutput('rejected', {
                    'rejection_id': str(uuid.uuid4()), # <--- ADDED THIS
                    'trade_id': trade_id,
                    'rejection_reason': f"Schema Error: {str(e)}",
                    'raw_payload': json.dumps(record),
                    'ingest_timestamp': datetime.utcnow().isoformat()
                })
                return

            today_str = date.today().isoformat()

            # --- CHECK 1: Maturity ---
            is_past_maturity = False
            if trade.maturity_date and trade.maturity_date < today_str:
                is_past_maturity = True

                if not trade.is_historical_load:
                    yield beam.pvalue.TaggedOutput('rejected', {
                        'rejection_id': str(uuid.uuid4()), # <--- ADDED THIS
                        'trade_id': trade.trade_id,
                        'rejection_reason': f"Invalid Maturity: {trade.maturity_date} is in the past.",
                        'raw_payload': json.dumps(record),
                        'ingest_timestamp': datetime.utcnow().isoformat()
                    })
                    return

            # --- CHECK 2: Version ---
            cached_version = current_version_state.read() or 0

            if trade.version < cached_version:
                yield beam.pvalue.TaggedOutput('rejected', {
                    'rejection_id': str(uuid.uuid4()), # <--- ADDED THIS
                    'trade_id': trade.trade_id,
                    'rejection_reason': f"Stale Version: Incoming {trade.version} < Current {cached_version}",
                    'raw_payload': json.dumps(record),
                    'ingest_timestamp': datetime.utcnow().isoformat()
                })
                return

            # Valid Version - Update State
            if trade.version > cached_version:
                current_version_state.write(trade.version)

            # --- CHECK 3: Expiration Status ---
            final_status = trade.status
            if trade.is_historical_load and is_past_maturity:
                final_status = 'EXPIRED'
            elif trade.status == 'EXPIRED_RECORD':
                final_status = 'EXPIRED'

            # --- FINAL WRITE PREP ---
            output_payload = record.copy()
            output_payload['status'] = final_status
            output_payload['is_current'] = True

            if 'is_historical_load' in output_payload:
                del output_payload['is_historical_load']

            yield beam.pvalue.TaggedOutput('valid', output_payload)

        except Exception as e:
            logging.error(f"System Error processing {trade_id}: {e}")

# ==========================================
# 5. Pipeline Execution
# ==========================================
def run():
    parser = argparse.ArgumentParser()
    parser.add_argument('--input_topic', required=True)
    parser.add_argument('--valid_history_table', required=True)
    parser.add_argument('--valid_current_table', required=True)
    parser.add_argument('--rejected_table', required=True)
    parser.add_argument('--valid_schema_path', required=True)
    parser.add_argument('--rejected_schema_path', required=True)
    parser.add_argument('--dlq_bucket', required=True)
    parser.add_argument('--project', required=True)
    parser.add_argument('--temp_location', required=True)
    parser.add_argument('--region', required=True)
    parser.add_argument('--runner', default='DataflowRunner')

    known_args, pipeline_args = parser.parse_known_args()

    # Load Schemas
    valid_schema = load_schema_from_gcs(known_args.valid_schema_path, known_args.project)
    rejected_schema = load_schema_from_gcs(known_args.rejected_schema_path, known_args.project)

    options = PipelineOptions(pipeline_args)
    gcp_options = options.view_as(GoogleCloudOptions)
    gcp_options.project = known_args.project
    gcp_options.region = known_args.region
    gcp_options.temp_location = known_args.temp_location

    setup_options = options.view_as(SetupOptions)
    setup_options.save_main_session = True

    options.view_as(StandardOptions).streaming = True
    options.view_as(StandardOptions).runner = known_args.runner

    with beam.Pipeline(options=options) as p:

        # 1. Ingest & Key
        keyed_trades = (
                p
                | "ReadPubSub" >> beam.io.ReadFromPubSub(topic=known_args.input_topic)
                | "ParseAndKey" >> beam.ParDo(ParseAndKeyTrades())
        )

        # 2. Deduplicate
        dedup_results = (
                keyed_trades
                | "WindowForDedup" >> beam.WindowInto(window.FixedWindows(600))
                | "GroupById" >> beam.GroupByKey()
                | "SplitDupes" >> beam.ParDo(SplitDuplicates()).with_outputs('unique', 'duplicates')
        )

        # 3. Archive Duplicates
        (
                dedup_results.duplicates
                | "WindowDupes" >> beam.WindowInto(window.FixedWindows(60))
                | "FormatDupes" >> beam.Map(lambda x: json.dumps(x))
                | "WriteDupesToGCS" >> WriteToText(f"{known_args.dlq_bucket}/duplicate_trades/", num_shards=1)
        )

        # 4. Process
        processed = (
                dedup_results.unique
                | "ReKeyForState" >> beam.Map(lambda x: (x.get('trade_id'), x))
                | "ProcessTrades" >> beam.ParDo(ProcessTrades()).with_outputs('rejected', 'valid')
        )

        # 5. Write History
        history_write = (
                processed.valid
                | "WriteHistory" >> WriteToBigQuery(
            table=known_args.valid_history_table,
            schema={'fields': valid_schema}, # FIX: Wrap schema list in dictionary
            create_disposition=BigQueryDisposition.CREATE_IF_NEEDED,
            write_disposition=BigQueryDisposition.WRITE_APPEND,
            method=WriteToBigQuery.Method.STREAMING_INSERTS,
            insert_retry_strategy=RetryStrategy.RETRY_NEVER
        )
        )

        # ------------------------------------------------------------------
        # REMOVE OR COMMENT OUT THIS SECTION
        # We do not stream to 'Current' anymore. The Scheduled Query handles it.
        # 6. Write Current State
        # ------------------------------------------------------------------
        #current_write = (
        #        processed.valid
        #        | "WriteCurrent" >> WriteToBigQuery(
        #    table=known_args.valid_current_table,
        #    schema={'fields': valid_schema}, # FIX: Wrap schema list in dictionary
        #    create_disposition=BigQueryDisposition.CREATE_IF_NEEDED,
        #    write_disposition=BigQueryDisposition.WRITE_APPEND,
        #    method=WriteToBigQuery.Method.STREAMING_INSERTS,
        #    insert_retry_strategy=RetryStrategy.RETRY_NEVER
        #)
        #)

        # 7. Write Rejected
        rejected_write = (
                processed.rejected
                | "WriteRejectedBQ" >> WriteToBigQuery(
            table=known_args.rejected_table,
            schema={'fields': rejected_schema}, # FIX: Wrap schema list in dictionary
            create_disposition=BigQueryDisposition.CREATE_IF_NEEDED,
            write_disposition=BigQueryDisposition.WRITE_APPEND,
            insert_retry_strategy=RetryStrategy.RETRY_NEVER
        )
        )

        # 8. Handle Failures
        all_failures = (
                (history_write.failed_rows, rejected_write.failed_rows)
                | "FlattenFailures" >> beam.Flatten()
        )

        (
                all_failures
                | "WindowFailures" >> beam.WindowInto(window.FixedWindows(60))
                | "FormatFailures" >> beam.Map(lambda x: json.dumps(x))
                | "WriteGCSDLQ" >> WriteToText(f"{known_args.dlq_bucket}/insert_failures/", num_shards=1)
        )

if __name__ == '__main__':
    run()