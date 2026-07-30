# SignalWatch — Company Risk Intelligence Platform

[![Deploy to Render](https://render.com/images/deploy-to-render-button.svg)](https://render.com/deploy?repo=https://github.com/sahusumit093434-cyber/signal-watch)

SignalWatch is a data engineering and API service that ingests messy public-event feeds, validates and cleanses them, computes analytical risk scores for companies using Apache Spark, and exposes the intelligence via a high-performance REST API.

---

## 1. Overview

In risk intelligence, public news, reports, and feeds are often unstructured, filled with duplicate entries, inaccurate casing, missing optional attributes, and inconsistent date formats. 

SignalWatch solves this by:
* **Ingesting** data from a live paginated REST API and a static CSV sheet.
* **Cleansing and Normalizing** the raw inputs into a unified shape, rejecting unrecoverable items.
* **Deduplicating** records using strict field semantic matching.
* **Processing and Scoring** the clean data using **PySpark** to calculate recency-weighted company risk profiles.
* **Serving** the intelligence through a FastAPI backend, providing query capabilities, statistical summaries, and Matplotlib charts.

---

## 2. Architecture

The project has a modular, clean structure dividing concerns between collection, cleaning, distributed processing, APIs, and analytics:

```text
signalwatch/
├── app/
│   ├── __init__.py
│   ├── main.py                # FastAPI Server & Routes
│   ├── config.py              # Path and environmental configuration
│   ├── ingestion/
│   │   ├── __init__.py
│   │   ├── collector.py       # API client & CSV collector
│   │   └── cleaner.py         # Validation, normalizers & deduplication
│   ├── processing/
│   │   ├── __init__.py
│   │   └── spark_processor.py # PySpark calculation engine
│   └── utils/
│       ├── __init__.py
│       └── analytics.py       # NumPy aggregation & Matplotlib visualizer
├── data/
│   ├── raw/                   # Immutable raw data inputs
│   ├── cleaned/               # Cleansed, unique events
│   ├── rejected/              # Validation rejects with reasons
│   └── processed/             # PySpark processed risk reports
├── outputs/
│   └── top_company_risks.png  # Generated horizontal bar chart
├── tests/                     # Unit & Integration test suites
│   ├── __init__.py
│   ├── test_ingestion.py
│   ├── test_processing.py
│   └── test_api.py
├── requirements.txt           # Dependency definition
├── README.md                  # Documentation
└── .gitignore                 # Version control exclusions
```

---

## 3. Setup

### Prerequisites
* Python 3.9+ (Tested on Python 3.14.3)
* Java Runtime Environment (JRE) / JDK (A local JDK 26 is automatically unpacked and set up programmatically in `app/config.py` without requiring global system paths).

### Installation & Execution

1. **Start the Mock API Server**:
   From the project root directory, run the mock API server:
   ```bash
   python mock_api.py
   ```
   Keep this terminal open (runs on `http://localhost:9000`).

2. **Set up Virtual Environment**:
   In a new terminal window:
   ```powershell
   python -m venv .venv
   .\.venv\Scripts\activate
   ```

3. **Install Dependencies**:
   ```bash
   pip install -r requirements-local.txt
   ```

4. **Run Unit Tests**:
   Verify everything passes:
   ```bash
   pytest
   ```

5. **Start the REST API**:
   ```bash
   uvicorn app.main:app --reload --port 8000
   ```
   The FastAPI swagger docs are interactive and accessible at `http://127.0.0.1:8000/docs`.

---

## 4. API Docs

The application exposes the following REST endpoints:

### `GET /health`
Returns system status.
* **Response**: `{"status": "healthy"}`

### `POST /ingest`
Triggers the full pipeline: fetches page-wise data from the mock API and CSV, normalizes, deduplicates, executes the PySpark risk scoring engine, updates caches, and builds NumPy stats & Matplotlib visuals.
* **URL Parameter**: `reference_date` (Optional, defaults to `"2026-07-24"` for reproducible scoring metrics).
* **Response**:
  ```json
  {
    "status": "completed",
    "received_records": 150,
    "valid_records": 132,
    "rejected_records": 8,
    "duplicate_records": 10,
    "companies_processed": 24
  }
  ```

### `GET /companies`
Query computed company scores.
* **Query Parameters**:
  * `risk_level` (Optional): Filter by `LOW`, `MEDIUM`, or `HIGH`.
  * `country` (Optional): Filter by company that has events in that country.
  * `minimum_score` (Optional): Filter by score >= value.
  * `limit` (Optional): Limit results count (default 100).
* **Response**: List of companies with their risk profile.

### `GET /companies/{company_name}`
Get details for a single company.
* **Response**:
  ```json
  {
    "name": "Northstar Logistics",
    "risk_score": 52.4,
    "risk_level": "MEDIUM",
    "event_count": 6,
    "top_categories": ["supply_chain", "cybersecurity"]
  }
  ```

### `GET /events`
Retrieves events with strict filtering rules. **Must provide at least two parameters.**
* **Query Parameters** (Provide at least 2):
  * `company` (Optional): Filter by company name.
  * `category` (Optional): Filter by category.
  * `min_score` (Optional): Event risk score >= value.
  * `country` (Optional): Filter by country.
  * `start_date` / `end_date` (Optional): Start and End ISO dates.
* **Response**: List of matching events with calculated event-level risk scores.

---

## 5. Risk-Scoring Explanation

### Event-Level Risk Score
For each individual event:
$$\text{Event Risk Score} = \min(100, \text{Severity} \times 20 \times \text{Confidence} \times \text{Recency Weight})$$

Where **Recency Weight** represents the age of the event relative to the pipeline execution date:
* **0–7 days old**: Weight = `1.0`
* **8–30 days old**: Weight = `0.8`
* **More than 30 days old**: Weight = `0.6`

### Company-Level Risk Score
$$\text{Company Risk Score} = \text{Average of the company's 10 highest event risk scores}$$
* If a company has **fewer than ten events**, we average all valid events.
* Scores are rounded to **2 decimal places**.

### Risk Classification
* **LOW**: `0.0` – `39.99`
* **MEDIUM**: `40.0` – `69.99`
* **HIGH**: `70.0` – `100.0`

---

## 6. Deduplication Strategy

The system classifies an incoming record as a duplicate if it matches an existing record on:
$$\text{Company Name} + \text{Category} + \text{Description}$$

To prevent formatting discrepancies from skipping deduplication, fields are normalized before hashing:
1. **Company Name**: Stripped of double spaces, trailing symbols, and common corporate suffixes (e.g. `Ltd`, `Pvt Ltd`, `Inc.`, `Corp`, `Plc.`).
2. **Category**: Mapped to the 6 canonical categories.
3. **Description**: Stripped of all non-alphanumeric punctuation and lowercased.

**Conflict Resolution Strategy**:
When duplicate events are identified, we keep only the single **highest-quality signal**. We rank duplicates in order:
1. **Confidence** (Desc) — higher signal accuracy.
2. **Severity** (Desc) — higher impact details.
3. **Published Date** (Desc) — most recent data.

---

## 7. Assumptions
* **Timezone**: All timestamp fields in inputs default to UTC.
* **Reference Date**: By default, recency is computed against `2026-07-24` (the dataset issue date) to guarantee reproducible output matching the brief. However, it can be passed dynamically.
* **Categorization Suffixes**: Suffix removal covers typical abbreviations like `Ltd`, `Pvt Ltd`, `Plc`, `Co`, `Inc`, and `Corp`.

---

## 8. Known Limitations
* **Local Spark Execution**: While local Spark mode runs exceptionally, launching a JVM context incurs a cold startup overhead (~3-5s) on the `POST /ingest` call.
* **In-Memory Cache**: The FastAPI server caches the processed Spark datasets in memory to provide ultra-fast query endpoints (`GET /companies` and `GET /events`). If the pipeline is triggered dynamically via `POST /ingest`, the cache updates, but multi-worker clusters would require shared states.

---

## 9. What to Improve with More Time
* **Persistent Storage**: Replace simple file storage with SQLite or PostgreSQL to index and query events, removing in-memory cache sync limits.
* **Description NLP**: Use text similarity embeddings (like BERT or TF-IDF) instead of exact character normalization to identify semantically identical descriptions (e.g., matching "DOS outage hit server" with "Denial of service attack took down server").
* **API Pagination**: Implement pagination on `GET /companies` and `GET /events` endpoints to support larger real-world datasets.

---

## 10. Production Deployment

This project includes a `Dockerfile` setup designed for containerized deployment in the cloud (such as **Render Web Services**, **Railway**, or **Fly.io**).

### Deployment Steps (e.g. Render)
1. Go to [Render Dashboard](https://dashboard.render.com).
2. Click **New** -> **Web Service**.
3. Connect your GitHub repository: `https://github.com/sahusumit093434-cyber/signal-watch.git`.
4. Render will automatically detect the `Dockerfile` in the root. 
5. Set:
   * **Runtime**: `Docker`
   * **Instance Type**: Select at least 2 GB of RAM (PySpark requires memory for JVM initialization).
6. Click **Deploy Web Service**.

When deployed, the container starts:
1. The **Mock API Server** on background port `9000` (serving paginated events).
2. The **FastAPI API Service** on foreground port `8000` (routing queries for companies, scores, and events).

---

## AI Tools Used
* **Gemini 3.5 Flash** (via Antigravity assistant) for planning layouts, designing Spark window aggregations, structuring Python class divisions, and writing test scenarios.

