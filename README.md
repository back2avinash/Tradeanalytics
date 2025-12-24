# Real-Time Trade Processing Pipeline on GCP

![Python](https://img.shields.io/badge/Python-3.x-blue?style=flat&logo=python)
![GCP](https://img.shields.io/badge/Google_Cloud-Platform-red?style=flat&logo=google-cloud)
![Apache Beam](https://img.shields.io/badge/Apache-Beam-orange?style=flat&logo=apache)
![Apache Airflow](https://img.shields.io/badge/Apache-Airflow-teal?style=flat&logo=apache-airflow)

## 📖 Project Overview

This project implements a scalable, **real-time ETL (Extract, Transform, Load) pipeline** for processing financial trade data using Google Cloud Platform (GCP).

The system simulates trade events, ingests them via **Cloud Pub/Sub**, processes them using **Apache Beam** (running on Dataflow), and orchestrates the workflow using **Apache Airflow** (Cloud Composer). The pipeline validates trades against business rules—specifically checking maturity dates—and routes them to different Cloud Storage buckets based on their validity.

---

## 🏗️ Architecture

The data flows through the system as follows:

```mermaid
graph LR
    A[Local Script] -->|JSON Events| B(Cloud Pub/Sub)
    C[Airflow DAG] -.->|Triggers| D{Dataflow Pipeline}
    B --> D
    D -->|Validate & Parse| E{Logic Check}
    E -->|Valid| F[GCS Bucket: Valid]
    E -->|Expired| G[GCS Bucket: Rejected]
