# Real-Time Trade Processing Pipeline on GCP

![Python](https://img.shields.io/badge/Python-3.x-blue?style=flat&logo=python)
![GCP](https://img.shields.io/badge/Google_Cloud-Platform-red?style=flat&logo=google-cloud)
![Apache Beam](https://img.shields.io/badge/Apache-Beam-orange?style=flat&logo=apache)
![Apache Airflow](https://img.shields.io/badge/Apache-Airflow-teal?style=flat&logo=apache-airflow)
![BigQuery](https://img.shields.io/badge/Google-BigQuery-blueviolet?style=flat&logo=google-cloud)

## 📖 Project Overview

This project implements a scalable, **event-driven ETL (Extract, Transform, Load) pipeline** for processing financial trade data using Google Cloud Platform (GCP).

The system simulates high-throughput trade events and ingests them via **Cloud Pub/Sub**. A streaming **Apache Beam** job (Dataflow) validates and processes the data, writing windowed files to **Cloud Storage (GCS)**.

To ensure decoupling and scalability, the loading into the Data Warehouse is event-driven: a **Cloud Run Function** is triggered automatically when new files land in GCS, loading the data into **BigQuery** for analysis.

---

## 🏗️ Architecture

The following diagram illustrates the event-driven flow from simulation to analytics.

```mermaid
graph LR
    %% Definitions for styling
    classDef compute fill:#e8f0fe,stroke:#4285f4,stroke-width:2px;
    classDef storage fill:#e6f4ea,stroke:#34a853,stroke-width:2px;
    classDef messaging fill:#fce8e6,stroke:#ea4335,stroke-width:2px;
    classDef orchestrate fill:#fef7e0,stroke:#fbbc05,stroke-width:2px;
    classDef local fill:#f1f3f4,stroke:#9aa0a6,stroke-width:2px,stroke-dasharray: 5 5;

    subgraph Source [Trade Source]
        A[Local Python Script]:::local
    end

    subgraph Ingestion [Ingestion & Orchestration]
        C(Cloud Composer):::orchestrate
        B(Cloud Pub/Sub):::messaging
    end

    subgraph Process [Processing]
        D{{Cloud Dataflow}}:::compute
    end

    subgraph Sinks [Storage & Analytics]
        F[GCS: Valid]:::storage
        G[GCS: Rejected]:::storage
        H(Cloud Run Function):::compute
        E[BigQuery]:::storage
    end

    %% Data Flow Connections
    A -->|JSON Events| B
    C -.->|Trigger| D
    B -->|Stream| D

    %% Processing Logic Flows
    D -->|Write Windowed Files| F
    D -->|Write Windowed Files| G
    
    %% Event Driven Load
    F -.->|Object Finalize Trigger| H
    G -.->|Object Finalize Trigger| H
    H -->|Load Job| E
