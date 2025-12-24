Real-Time Trade Processing Pipeline on GCP
Project Overview
This project implements a scalable, real-time ETL (Extract, Transform, Load) pipeline for processing financial trade data using Google Cloud Platform (GCP).

The system simulates trade events, ingests them via Cloud Pub/Sub, processes them using Apache Beam (running on Dataflow), and orchestrates the workflow using Apache Airflow (Cloud Composer). The pipeline validates trades against business rules (specifically checking maturity dates) and routes them to different Cloud Storage buckets based on their validity.

Architecture
The data flows through the system as follows:

Ingestion: A local Python script generates mock trade data (JSON) and publishes it to a Pub/Sub topic (trade-events).

Orchestration: An Airflow DAG triggers a streaming Dataflow job.

Processing (ETL): The Apache Beam pipeline:

Reads streaming data from Pub/Sub.

Decodes and parses the JSON payloads.

Validation Logic: Checks if the maturity_date is in the past.

Branching: Splits traffic into two streams: Valid and Rejected.

Windowing: Batches data into 60-second fixed windows.

Storage: Writes the processed results to Google Cloud Storage (GCS) with dynamic filenames based on window timestamps.

Key Features
Streaming Analytics: Processes data in real-time rather than batches.

Event Simulation: Includes a generator that creates valid trades, expired trades, and updates to test pipeline logic.

Data Validation: Automatically rejects trades where the maturity date is prior to the current date.

Infrastructure as Code: Uses Python for both pipeline logic (Beam) and orchestration (Airflow).

File Structure
main.py: The entry point for the simulation. It detects the GCP project, initializes the Pub/Sub manager, and sends a batch of test events (valid, past maturity, and updates).

pubsub_manager.py: A helper class that wraps the google-cloud-pubsub client to handle topic connections and message publishing.

generate_trade_payload.py: Contains the logic to create mock trade data with specific attributes (Trade ID, Version, Maturity Date).

trade_processing_orchestration.py: An Airflow DAG file designed for Cloud Composer. It configures and triggers the Dataflow job using BeamRunPythonPipelineOperator in streaming mode.

trade_processor_pipeline.py: The core Apache Beam pipeline script. It defines the ValidateTrade DoFn, handles the windowing strategy, and manages writing to GCS.

Technologies Used
Language: Python 3

Google Cloud Platform:

Cloud Pub/Sub (Messaging)

Cloud Dataflow (Streaming Processing)

Cloud Storage (Data Lake/Output)

Cloud Composer (Orchestration)

Libraries: apache-beam[gcp], google-cloud-pubsub, apache-airflow

Prerequisites
To run this project, you need:

Active GCP Project with Dataflow and Pub/Sub APIs enabled.

A Service Account with permissions to access Pub/Sub, Dataflow, and GCS.

Google Cloud SDK installed locally.
