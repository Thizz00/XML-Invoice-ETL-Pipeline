<img src="https://img.shields.io/badge/Apache%20Airflow-017CEE?logo=apacheairflow&logoColor=fff"/> <img src="https://img.shields.io/badge/Apache%20Spark-E25A1C?logo=apachespark&logoColor=fff"/> <img src="https://img.shields.io/badge/Docker-2496ED?logo=docker&logoColor=fff"/> <img src="https://img.shields.io/badge/Postgres-%23316192.svg?logo=postgresql&logoColor=white"/>

## XML Invoice ETL Pipeline
Scalable Data Pipeline designed to validate, process, and load XML invoices into a PostgreSQL database using Apache Airflow, PySpark, and Docker.

### Architecture
The system follows a decoupled architecture orchestrated by Airflow:

- Validation (Airflow/LXML): Pre-screens XML files against XSD schema.

- Processing (PySpark): A containerized Spark job parses valid XMLs, performs transformations, and handles schema mapping.

- Storage (PostgreSQL): Final data is stored using an idempotent Upsert logic (Atomic Transactions).

### Quick Start
Build & Launch:

```bash
docker-compose up -d --build
```

Run Pipeline: Enable and trigger invoice_pipeline in Airflow UI.
