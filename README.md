# 🏛 Procurement Watchdog Lakehouse

A production-grade, extensible lakehouse pipeline for public procurement data.

This project implements an automated, incremental data engineering system that ingests public procurement notices (initially from Poland’s BZP), processes them through a bronze/silver/gold lakehouse architecture, validates data quality, computes competition and concentration metrics, and publishes analytics artifacts daily.

The system is designed as a modular, extensible platform — not a one-off analysis.

---

## 🎯 Design Principles

* **Incremental ingestion**
* **Idempotent processing**
* **Reproducible runtime (Dockerized Spark environment)**
* **Schema-first validation**
* **Layered lakehouse architecture**
* **Stateless automation via GitHub Actions**
* **Pluggable ingestion and transformation modules**
* **Source-agnostic design (multi-country extensibility)**

---

## 🏗 System Architecture

```
             ┌─────────────────────┐
             │   Public Data API   │
             └──────────┬──────────┘
                        ↓
        Incremental Fetch + Validation (Pydantic)
                        ↓
                Bronze (Raw Parquet)
            Versioned via GitHub Releases
                        ↓
            Spark Transformations (Silver)
        Schema enforcement + Pandera validation
                        ↓
          Analytical Marts + Risk Signals (Gold)
                        ↓
      Publish to GitHub Pages (DuckDB analytics)
```

Storage:

* **Bronze & Silver** → Parquet (GitHub Releases as object storage)
* **Gold** → committed to GitHub Pages repository
* **State** → lightweight incremental state file

---

## 🗃 Lakehouse Layers

### 🥉 Bronze — Raw Ingestion Layer

Characteristics:

* Minimal transformation
* Append-only
* Immutable
* Partitioned by ingestion date
* Schema validated at record level (Pydantic)

Responsibilities:

* Preserve source integrity
* Enable reproducibility
* Allow schema evolution tracking

---

### 🥈 Silver — Conformed Data Layer

Characteristics:

* Type normalization
* Deduplication
* Canonicalized schema
* Data quality enforcement via Pandera
* Deterministic transformations

Responsibilities:

* Ensure analytical consistency
* Provide clean, joinable datasets
* Abstract source-specific quirks

---

### 🥇 Gold — Analytical Marts

Characteristics:

* Aggregated metrics
* Rolling-window baselines
* Concentration metrics (HHI, top-share)
* Risk scoring signals
* Alert datasets

Responsibilities:

* Provide analysis-ready outputs
* Enable downstream reporting
* Serve as contract for dashboard layer

---

## 🚨 Risk & Signal Framework

The system computes statistically grounded signals such as:

* Single-bid rate spikes (rolling median + MAD)
* Buyer-level competition degradation
* Vendor concentration increases
* Publication bursts
* Value outliers vs historical baselines

The architecture is designed to support future expansion to:

* Distribution drift detection
* Multi-dimensional anomaly scoring
* Probabilistic risk modeling

---

## ⚙️ Technology Stack

Core Processing:

* **Python 3.11**
* **PySpark**
* **Parquet (lakehouse storage)**

Validation:

* **Pydantic** – ingestion schema enforcement
* **Pandera** – DataFrame-level validation
* (Planned) **Great Expectations** – full data quality suite

Infrastructure:

* **Docker** – reproducible Spark runtime
* **GitHub Actions** – scheduled execution
* **GitHub Releases** – object storage for lake layers
* **GitHub Pages** – published analytics
* **DuckDB** – lightweight analytical query engine (presentation layer)

---

## 🔄 Incremental Processing Strategy

The pipeline:

* Tracks last successful ingestion
* Fetches only new records
* Ensures idempotent writes
* Handles schema drift defensively
* Produces deterministic outputs

This enables daily scheduled execution without persistent infrastructure.

---

## 🔌 Extensibility & Future Development

The system is intentionally modular.

### 1️⃣ Multi-Source Ingestion

The ingestion layer can be extended to support:

* EU TED data
* Other national procurement systems
* Regional open data APIs
* Contract registers
* Court or regulatory datasets

Each source can implement a dedicated adapter module while reusing silver/gold logic.

---

### 2️⃣ NLP & Textual Analysis (Planned)

Future extensions may include:

* NLP-driven classification of procurement descriptions
* Topic modeling across contracting authorities
* Similarity detection between specifications
* Vendor clustering based on awarded contracts
* Semantic anomaly detection

The lakehouse structure supports enrichment fields without architectural changes.

---

### 3️⃣ Additional Analytical Capabilities

* Cross-country benchmarking
* Longitudinal competition trends
* Procedure-type structural shifts
* Dynamic thresholding
* Risk score calibration

---

## 📦 Repository Structure

```
src/procurement/
  ingest/
  bronze/
  silver/
  gold/
  publish/

docker/
.github/workflows/
sql/
tests/
state/
```

---

## 🧪 Local Execution

```bash
docker build -t procurement-pipeline .
docker run procurement-pipeline
```

Or:

```bash
python scripts/run_pipeline.py
```

---

## 🛠 Why This Project

This repository demonstrates:

* Production-grade batch architecture
* Schema validation at multiple layers
* Idempotent incremental ingestion
* Object-storage-based lakehouse design
* CI/CD-driven data workflows
* Transparent publication of derived analytics

It is designed as a realistic, extensible data engineering system suitable for further expansion into a multi-country procurement analytics platform.

---

## 📜 Disclaimer

This system analyzes publicly available procurement data for transparency and research purposes.
It provides statistical signals, not legal or investigative conclusions.
