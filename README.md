# Brewery Medallion Pipeline (Airflow)

Pipeline that ingests data from Open Brewery DB API and writes to a local data lake using medallion architecture:
- **Bronze:** raw data snapshot (JSONL)
- **Silver:** curated data (Parquet) partitioned by location
- **Gold:** aggregated view

## Run with Docker
1) Copy env file:
```bash
cp .env.example .env
```

2) Initialize Airflow:
> Prerequisite (Windows): Docker Desktop must be running (Linux containers / WSL2).

```bash
make airflow-init
```

3) Start services:
```bash
make airflow-up
```

4) Access airflow:
Airflow UI: http://localhost:8080
Credentials are set in .env
