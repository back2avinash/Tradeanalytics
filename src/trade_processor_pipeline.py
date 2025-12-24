import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions
from apache_beam.transforms.window import FixedWindows
from apache_beam.io import fileio  # <--- NEW IMPORT
from datetime import datetime
import sys
import logging
import json

# <--- NEW: Naming function defined at top level
def trade_filename_naming(window, pane, shard_index, total_shards, compression, destination):
    """
    Generates filenames based on the Window time.
    Format: trade-YYYYMMDD-HHMMSS.json
    """
    ts = window.start.to_utc_datetime().strftime("%Y%m%d-%H%M%S")
    # We include shard_index to prevent overwrites if multiple workers write at once
    return f"trade-{ts}-{shard_index}.json"

class ValidateTrade(beam.DoFn):
    def process(self, element, *args, **kwargs):
        try:
            # element from PubSub is bytes, decode it
            trade = json.loads(element.decode('utf-8'))
            today = datetime.now().date()
            maturity_date = datetime.strptime(trade['maturity_date'], '%Y-%m-%d').date()

            if maturity_date < today:
                trade['rejection_reason'] = "Maturity date in past"
                yield beam.pvalue.TaggedOutput('rejected', json.dumps(trade))
            else:
                trade['status'] = 'EXPIRED' if maturity_date == today else 'VALID'
                yield beam.pvalue.TaggedOutput('valid', json.dumps(trade))
        except Exception as e:
            logging.error(f"Error processing element: {e}")

def run():
    pipeline_options = PipelineOptions(flags=sys.argv)
    google_cloud_options = pipeline_options.view_as(GoogleCloudOptions)
    project_id = google_cloud_options.project

    if not project_id:
        raise ValueError("Project ID is missing.")

    INPUT_TOPIC = f"projects/{project_id}/topics/trade-events"
    OUTPUT_BUCKET = f"gs://trade-outputs-{project_id}"

    pipeline_options.view_as(beam.options.pipeline_options.SetupOptions).save_main_session = True
    pipeline_options.view_as(beam.options.pipeline_options.StandardOptions).streaming = True

    # REMOVED: ts_with_ms = datetime.now().strftime(...)
    # (Reason: This created a static timestamp for the whole job lifetime)

    with beam.Pipeline(options=pipeline_options) as p:
        events = p | "ReadFromPubSub" >> beam.io.ReadFromPubSub(topic=INPUT_TOPIC)

        validated = events | "ValidateRules" >> beam.ParDo(ValidateTrade()).with_outputs('valid', 'rejected')

        # 1. Valid Trades Path
        (validated.valid
         | "WindowValid" >> beam.WindowInto(FixedWindows(60))
         | "WriteValid" >> fileio.WriteToFiles(
                    path=f"{OUTPUT_BUCKET}/valid/",
                    sink=fileio.TextSink(),
                    file_naming=trade_filename_naming
                ))

        # 2. Rejected Trades Path
        (validated.rejected
         | "WindowRejected" >> beam.WindowInto(FixedWindows(60))
         | "WriteRejected" >> fileio.WriteToFiles(
                    path=f"{OUTPUT_BUCKET}/rejected/",
                    sink=fileio.TextSink(),
                    file_naming=trade_filename_naming
                ))

if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()