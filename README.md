# Big Data Taxi Analytics

A modern analytics platform for taxi trip data, featuring an ETL pipeline, Delta Lake storage, and a FastAPI-powered REST API for analytics.

---

## Features

- **ETL Pipeline**: Extracts, transforms, and loads taxi trip data into Delta Lake tables.
- **Delta Lake Storage**: Reliable, versioned storage for analytics.
- **Analytics**: Provides summary statistics and top pickup locations via API.
- **REST API**: FastAPI application exposes analytics endpoints.
- **Configuration Driven**: All settings (Spark, paths, API) are managed via YAML files in the `config/` directory.
- **Logging**: Centralized logging for ETL and API.
- **Clean Architecture**: Separation of ETL, analytics, and API layers.
- **Containerized**: Docker and Docker Compose support for easy deployment.

---

## Project Structure

```
big_data_analytics/
│
├── config/                  # YAML configuration files
│   ├── api_config.yaml
│   ├── paths.yaml
│   └── spark_config.yaml
│
├── data/                    # Place your raw taxi data here (e.g., taxi_data.csv)
│
├── src/
│   ├── api/                 # FastAPI application and routes
│   ├── analytics/           # Analytics logic (summary stats, top locations)
│   ├── config_loader/       # Config loading utilities
│   └── utils/               # Logger and helpers
│
├── data_generator/          # Data generator scripts
│   └── generate.py
│
├── test/                    # Test scripts
│   └── test_analytics.py
│
├── docker-compose.yaml
├── requirements.txt
└── README.md
```

---

## Getting Started

### 1. Prerequisites

- Python 3.8+
- [Docker](https://www.docker.com/products/docker-desktop) (optional, for containerized setup)

---

### 2. Setup

1. **Clone the repository:**
    ```bash
    git clone <your-repo-url>
    cd Big_Data_Analytics
    ```

2. **Add Data (Optional):**
    - Place your taxi trip data CSV file in the `data/` directory and name it `taxi_data.csv`.
    - Or, use the provided data generator to simulate data (see below).

3. **Install dependencies (if running locally):**
    ```bash
    pip install -r requirements.txt
    ```

---

### 3. Configuration

- All configuration is managed via YAML files in the `config/` directory:
    - `spark_config.yaml`: Spark and Delta Lake settings
    - `paths.yaml`: Data and Delta Lake paths
    - `api_config.yaml`: API metadata

---

### 4. Running the ETL Process

- Run your ETL script to populate the Delta Lake table before using the analytics API.
- (See `src/etl/` or your ETL script location. **Make sure Delta Lake data is written to the path specified in `paths.yaml`**.)

---

### 5. Running the API

**Locally:**
```bash
uvicorn src.api.main:app --reload
```

**With Docker Compose:**
```bash
docker-compose up --build
```

---

### 6. Generating Data (Optional)

You can use the provided data generator to simulate taxi trip data and send it to the API ingestion endpoint:

```bash
python data_generator/generate.py
```

- This will continuously send batches of simulated taxi trip records to the API at `/ingest/trip`.
- Make sure the API server is running before starting the generator.

---

### 7. Accessing the API

- **Interactive Docs:** [http://localhost:8000/docs](http://localhost:8000/docs)
- **Endpoints:**
    - `GET /analytics/summary` — Get overall statistics.
    - `GET /analytics/top-pickup-locations?limit=5` — Get the top 5 pickup locations.

---

### 8. Testing the Analytics API

A test script is provided:

```bash
python test/test_analytics.py
```

This will print the results of the analytics endpoints.

---

## Notes

- Ensure your Delta Lake data is available at the path specified in `config/paths.yaml` before querying analytics endpoints.
- Update configuration files as needed for your environment.
- The data generator is useful for simulating a live data stream and testing the ingestion and analytics pipeline end-to-end.

---

Author : Mohammad Dayarneh 
