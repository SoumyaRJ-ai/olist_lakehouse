# Olist Lakehouse Project  
**Medallion Architecture with CDC, SCD Type 2, and Incremental Processing**

---

## 📌 Overview

This project demonstrates the design and implementation of a production-shaped Data Lakehouse using the Medallion Architecture (Bronze → Silver → Gold) on Databricks.

The objective is not just to ingest data, but to design a pipeline that is:

- Idempotent  
- Incremental  
- Layered with clear responsibility  
- Auditable  
- Modeled using star schema principles  

The dataset represents an e-commerce domain consisting of customers, orders, order items, and products.

---

## 🏗 Architecture

![Architecture Diagram](./diagrams/olist_lakehouse_architecture.png)

---

## 🟤 Bronze Layer – Raw Preservation

**Purpose:** Preserve source data faithfully.

**Characteristics:**
- Append-only Delta tables  
- Explicit schema casting  
- Ingestion metadata (`ingestion_ts`, `source_file`)  
- No business logic  

**Tables:**
- customers  
- orders  
- order_items  
- products  

Bronze ensures traceability and serves as the single source of truth.

---

## ⚪ Silver Layer – Behavioral Modeling

**Purpose:** Model data state and business behavior.

### Orders → CDC (Type 1)
- MERGE-based incremental processing  
- Latest state maintained  
- Idempotent re-runs  

### Customers → SCD Type 2
- Historical tracking of changes  
- `effective_from` / `effective_to` timestamps  
- `is_current` flag  
- Two-step expire-and-insert strategy  

Products and order_items are treated as immutable in this implementation.

---

## 🟡 Gold Layer – Analytics Star Schema

**Purpose:** Provide analytics-ready, consumption-safe data.

### Dimensions
- `dim_customers` (SCD Type 2)  
- `dim_products` (Type 1)  

### Fact
- `fact_sales`  
- Grain: `(order_id, order_item_id)`  
- Surrogate keys: `customer_sk`, `product_sk`  
- MERGE-based incremental load  

### Data Quality
- `fact_sales_rejects` captures invalid or filtered records  
- No silent data loss  

Gold enforces a clear separation between descriptive attributes (dimensions) and measurable events (fact).

---

## 🔄 Incremental Processing Strategy

The pipeline is designed to be safely re-runnable.

- Bronze: Append-only ingestion  
- Silver Orders: MERGE-based CDC  
- Silver Customers: Two-step SCD Type 2 implementation  
- Gold Fact: MERGE on composite grain  

All transformations are idempotent and support incremental updates.

---

## 📊 Data Modeling Highlights

- Clear layer responsibility separation  
- Star schema modeling  
- Strict fact grain definition  
- Surrogate key usage in dimensions  
- Explicit reject handling for data quality transparency  

---

## ⚙ Configuration

The project uses a centralized configuration file:

```
config/config.yaml
```

Configuration includes:

- Schema names (bronze, silver, gold)  
- Raw data paths  
- Table names  

This prevents hard-coded values inside transformation scripts and improves maintainability.

---

## 📁 Project Structure

```
olist-lakehouse/
│
├── bronze/
├── silver/
├── gold/
├── config/
│   └── config.yaml
├── diagrams/
│   └── olist_lakehouse_architecture.png
├── README.md
├── requirements.txt
└── .gitignore
```

---

## 🛠 Tech Stack

- Databricks  
- Apache Spark (PySpark)  
- Delta Lake  
- Medallion Architecture  
- Star Schema Modeling  

---

## 🧪 Reproducibility

All configurations are centralized under `config/config.yaml`.  
No hardcoded paths or schema names exist in transformation logic.

---

## ⚙ Execution Environment

This project is designed to run within a Databricks environment where a Spark session is already available.

Although the repository is structured as a clean Python project for clarity and modularity, execution assumes:

- An active Spark session  
- Delta Lake support  
- Databricks Runtime  

The `.py` structure improves maintainability and version control, while execution remains cluster-based.

This repository focuses on architectural clarity and production-oriented design rather than local standalone execution.

---

## 🚀 Future Enhancements

- Cloud-based incremental ingestion with folder-level detection  
- Partitioning and performance optimization  
- Automated orchestration (Airflow / Workflows)  
- Data validation framework  

---

## 💡 Design Principles

- Clear layer ownership  
- Idempotent processing  
- Incremental MERGE logic  
- Explicit data quality handling  
- Separation of ingestion, transformation, and analytics concerns  

---

## 🎯 Summary

This project demonstrates how backend engineering rigor can be applied to modern data engineering:

- Deterministic behavior  
- Explicit state management (CDC & SCD2)  
- Controlled schema modeling  
- Transparent data quality handling  

The result is a structured, production-oriented lakehouse implementation rather than a simple ETL script collection.
