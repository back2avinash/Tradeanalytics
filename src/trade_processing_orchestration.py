import os
from airflow import DAG
from airflow.providers.apache.beam.operators.beam import BeamRunPythonPipelineOperator
from datetime import datetime
import time

# Cloud Composer automatically sets GCP_PROJECT in the environment
PROJECT_ID = os.environ.get("GCP_PROJECT", "trade-analytics-481714")

with DAG(
        "trade_pipeline_orchestration",
        # Use a static start date in the past to ensure it's ready to run
        start_date=datetime(2023, 12, 19),
        # Change schedule to '@once' to trigger immediately upon upload
        schedule='@once',
        catchup=False
) as dag:

    run_etl = BeamRunPythonPipelineOperator(
        task_id="start_trade_etl",
        py_file="gs://dag_tradeanalytics/scripts/trade_processor_pipeline.py",
        runner="DataflowRunner",

        pipeline_options={
            "project": PROJECT_ID,
            "region": "asia-south2",
            "job_name": f"trade-processing-job-{int(time.time())}",
            "streaming": True,
            "staging_location": f"gs://dag_tradeanalytics/staging/",
            "temp_location": f"gs://dag_tradeanalytics/temp/",
            'service_account_email': 'dataflow-worker-sa@trade-analytics-481714.iam.gserviceaccount.com'
        },

        py_interpreter="python3",
        py_requirements=["apache-beam[gcp]"],
        py_system_site_packages=False,

        # CRITICAL for Streaming Jobs
        # this MUST be set this to False. Otherwise, Airflow waits for the
        # streaming job to finish (which it never does), blocking DAG forever.
        dataflow_config={"wait_until_finished": False},
    )