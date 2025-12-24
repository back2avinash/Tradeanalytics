# Real-Time Trade Processing Pipeline on GCP

![Python](https://img.shields.io/badge/Python-3.x-blue?style=flat&logo=python)
![GCP](https://img.shields.io/badge/Google_Cloud-Platform-red?style=flat&logo=google-cloud)
![Apache Beam](https://img.shields.io/badge/Apache-Beam-orange?style=flat&logo=apache)
![Apache Airflow](https://img.shields.io/badge/Apache-Airflow-teal?style=flat&logo=apache-airflow)
![BigQuery](https://img.shields.io/badge/Google-BigQuery-blueviolet?style=flat&logo=google-cloud)

## 📖 Project Overview

This project implements a scalable, **real-time ETL (Extract, Transform, Load) pipeline** for processing financial trade data using Google Cloud Platform (GCP).

The system simulates high-throughput trade events and ingests them via **Cloud Pub/Sub**. A streaming **Apache Beam** job, running on **Cloud Dataflow**, processes these events in real-time. The workflow is orchestrated and monitored using **Apache Airflow** (Cloud Composer).

The pipeline validates trades against business rules (specifically checking maturity dates) and routes data to two destinations: **Cloud Storage (GCS)** for archival data lakes and **BigQuery** for real-time reporting and analytics.

---

## 🏗️ Architecture

The following diagram illustrates the end-to-end flow of trade data from simulation to analytics.

```mermaid
graph TD
    %% Definitions for styling to simulate professional service grouping
    classDef compute fill:#e8f0fe,stroke:#4285f4,stroke-width:2px;
    classDef storage fill:#e6f4ea,stroke:#34a853,stroke-width:2px;
    classDef messaging fill:#fce8e6,stroke:#ea4335,stroke-width:2px;
    classDef orchestrate fill:#fef7e0,stroke:#fbbc05,stroke-width:2px;
    classDef local fill:#f1f3f4,stroke:#9aa0a6,stroke-width:2px,stroke-dasharray: 5 5;

    subgraph "Trade Simulation Source"
        A[Local Python Producer]:::local
    end

    subgraph "Google Cloud Platform"
        subgraph "Ingestion & Orchestration"
            B(Cloud Pub/Sub<br/>Topic: trade-events):::messaging
            C(Cloud Composer<br/>Apache Airflow):::orchestrate
        end

        subgraph "Streaming ETL Processing"
            D{{Cloud Dataflow<br/>Apache Beam Pipeline}}:::compute
        end

        subgraph "Data Sinks & Analytics"
            E[Google BigQuery<br/>Valid Trade Table]:::storage
            F[Cloud Storage<br/>Valid Archival Bucket]:::storage
            G[Cloud Storage<br/>Rejected/DLQ Bucket]:::storage
        end
    end

    %% Flows
    A -->|JSON Messages| B
    C -.->|Triggers Streaming Job| D
    B -->|Streaming Subscription| D

    %% Dataflow Internal Logic Flows
    D == Validated Trades ==> E
    D -- Windowed Batches<br/>(Valid) --> F
    D -- Windowed Batches<br/>(Expired/Invalid) --> G
