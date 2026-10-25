# Retail Lakehouse Analytics Platform 🛒

An end-to-end **Databricks Lakehouse** data engineering project that builds a production-style batch + streaming ELT pipeline for a retail business — covering raw ingestion, data quality enforcement, slow-changing dimensions, and orchestrated execution, all on the **Medallion Architecture** (Bronze → Silver → Gold).

Built entirely on Databricks: PySpark, Auto Loader, Delta Lake, Unity Catalog Volumes, Lakeflow Spark Declarative Pipelines, and Declarative Automation Bundles (DABs).

---

## 📌 Project Overview

The pipeline consolidates four retail data domains — **Stores**, **Products**, **Transactions**, and **Customers** — from raw files landing in Unity Catalog Volumes into clean, governed, query-ready Delta tables.

| Domain | Source Format | Ingestion Method | Highlights |
| --- | --- | --- | --- |
| Store records | CSV | Batch read with explicit schema | Corrupt-record capture, nested JSON parsing, phone/zip validation |
| Product records | JSONL | Batch read → Delta | Nested field flattening, price-band classification, SCD Type 2 |
| Transactions | CSV | **Auto Loader (streaming)** | Schema evolution, checkpointing, dedup, MERGE upserts |
| Customers | CSV | **Spark Declarative Pipeline** | Auto Loader bronze with rescued data, 6 data quality gates, SCD Type 1 via Auto CDC, gold aggregate materialized view |

---

## 🏗️ Architecture

```
                    ┌─────────────────────────────────────────────────┐
                    │   Unity Catalog Volumes (Landing Zone)          │
                    │   store_100_records.csv · product_1000.jsonl    │
                    │   transactions/*.csv · customers/*.csv          │
                    └────────────────────────┼────────────────────────┘
                                             │
                    ┌────────────────────────┼────────────────────────┐
                    │                        │                        │
             ┌──────▼──────┐          ┌──────▼──────┐          ┌──────▼──────┐
             │ Store       │          │ Product     │          │ Transaction │
             │ Batch Bronze│          │ Batch Bronze│          │ Auto Loader │
             └──────┬──────┘          └──────┬──────┘          │ (Streaming) │
                    │                        │                 └──────┬──────┘
             ┌──────▼──────┐          ┌──────▼──────┐          ┌──────▼──────┐
             │ Store       │          │ Product     │          │ Transaction │
             │ Silver      │          │ Silver +    │          │ Silver      │
             │ (cleaned)   │          │ SCD Type 2  │          │ (MERGE)     │
             └──────┬──────┘          └──────┬──────┘          └──────┬──────┘
                    └────────────────────────┼────────────────────────┘
                                             │
                                   ┌─────────▼─────────┐
                                   │  Customers SDP    │
                                   │  (SCD Type 1 +    │
                                   │   Gold MV)        │
                                   └───────────────────┘

                   Orchestrated end-to-end by a Lakeflow Job defined in a
                   Declarative Automation Bundle (PracticeProject.yaml)
```

### Orchestration flow (DAB-defined Lakeflow Job)

`Task 1: Store` + `Task 2: Product` → `Task 3: Transaction (Auto Loader)` → `Task 4: Customers (SDP pipeline)`

Tasks 1 and 2 run in parallel; the transaction load waits for both dimensions to be ready; the customer pipeline runs last — a classic dependency-driven ELT chain.

---

## 📂 Repository Structure

| File | Description |
| --- | --- |
| `store` | Store dimension: batch CSV ingestion with explicit schema and PERMISSIVE mode (corrupt rows captured in `_corrupt_record`), then silver cleaning — nested `manager_info`/`contact_info` JSON parsed into flat columns, zip/phone regex cleansing, deduplication, and phone validity flagging. |
| `product` | Product dimension: JSONL → Delta conversion, nested `inventory`/`supplier` flattening, unit-price derivation, and price-band categorization (`A+` to `D`). Includes a full **SCD Type 2** implementation via Delta `MERGE` (`start_date`, `end_date`, `is_current`) tracking product price history, plus duplicate-history verification queries. |
| `transaction_autoloader` | Streaming fact load: **Auto Loader** (`cloudFiles`) with schema inference, schema evolution (`addNewColumns`), checkpointing, and `Trigger.AvailableNow`. Silver layer parses JSON-in-CSV fields, applies safe `try_cast` type coercion, filters invalid/null records, deduplicates with window functions, and upserts via `MERGE`. Audited with `DESCRIBE HISTORY` to verify merge statistics. |
| `customers_dlt` | Lakeflow **Spark Declarative Pipeline** with a full medallion flow: bronze streaming table via Auto Loader with a rescued-data column for schema drift; an intermediate `customers_cleaned` table with **six `expect_or_drop` data quality gates** (NOT NULL customer/account/DOB/registration date, plus city/state validity that drops `##INVALID##` values); a silver **SCD Type 1** streaming table built with declarative Auto CDC (`dlt.apply_changes` keyed on `customer_no`, sequenced by `_ingested_at` — latest record wins, exactly one row per customer); and a gold materialized view `customers_gold` aggregating daily signups by city × account status with count, oldest/youngest DOB, and distinct occupation/segment/pincode measures. |
| `PracticeProject.yaml` | Declarative Automation Bundle defining the multi-task Lakeflow Job with task dependencies, Git source, queueing, and performance-optimized targets. |

---

## ✨ Key Techniques & Skills Demonstrated

* **Medallion Architecture** — raw immutable Bronze tables with lineage/audit columns (`_source_file`, `_ingested_at`), progressively cleaned Silver tables, and business-ready gold aggregates.
* **Streaming ingestion** — Auto Loader with `cloudFiles.format=csv`, schema location, schema evolution, and checkpointing for exactly-once semantics.
* **Incremental processing** — `trigger(availableNow=True)` batch-style streaming and `MERGE INTO` upserts for idempotent re-runs.
* **Data quality engineering** — PERMISSIVE read mode + corrupt-record columns, `_rescued_data` for schema drift, `try_cast`/`try_to_date` safe coercions, declarative `expect_or_drop` expectations, and multi-stage null/positivity filters.
* **Deduplication strategies** — `dropDuplicates`, and window-function (`row_number` over partition) latest-record selection.
* **Slowly Changing Dimensions** — SCD Type 2 price-history tracking with Delta Lake `MERGE` and current-record flagging; SCD Type 1 customer dimension via declarative Auto CDC (`dlt.apply_changes`).
* **Semi-structured data handling** — nested JSON columns parsed with `from_json` and explicit schemas, flattened into typed columns.
* **Data governance** — all tables registered under Unity Catalog (store/product/transaction tables in `workspace.training.*`, customer pipeline tables in `workspace.default.*`) with data stored in UC Volumes.
* **Orchestration & CI/CD** — dependency-based multi-task Lakeflow Jobs defined as code (DABs) and version-controlled in Git, syncing from GitHub to the Databricks workspace.
* **Observability** — Delta table history auditing (`DESCRIBE HISTORY`) with row-level merge statistics.

---

## 🛠️ Tech Stack

<div align="center">

| Layer | Technologies |
| --- | --- |
| Platform | Databricks (Runtime 18.2, Photon) |
| Processing | PySpark (Spark SQL, Structured Streaming) |
| Storage | Delta Lake, Unity Catalog Volumes |
| Streaming | Auto Loader (cloudFiles connector) |
| Pipelines | Lakeflow Spark Declarative Pipelines |
| Orchestration | Lakeflow Jobs, Declarative Automation Bundles (DABs) |
| Version Control | Git / GitHub |

</div>

---

## 🚀 How It Works

1. **Ingest (Bronze)** — Raw CSV/JSONL files land in UC Volumes. Batch jobs and Auto Loader streams load them into raw Delta tables, preserving source-file lineage and ingestion timestamps.
2. **Clean & Conform (Silver)** — Each notebook applies domain-specific cleansing: typed casts, JSON flattening, regex-based standardization, null filtering, and deduplication.
3. **Model (Slowly Changing Dimensions & Aggregates)** — Product prices are tracked over time with SCD Type 2 merges; transactions are incrementally upserted via `MERGE`; customers are deduplicated into an SCD Type 1 dimension via Auto CDC and rolled up into a gold aggregate materialized view.
4. **Orchestrate** — A single DAB-defined Lakeflow Job runs all tasks in dependency order (dimensions first, then facts, then the customer SDP), queue-enabled and performance-optimized.

---

## 📊 Sample Outcomes

* ~30,550 transaction records ingested, deduplicated, and upserted into the silver fact table.
* 100 store records and 1,000 product records cleaned, validated, and governed in Unity Catalog.
* Product SCD Type 2 history correctly maintains one current record per product with closed-out historical versions.
* ~200,000 raw customer records ingested via Auto Loader; 3,549 dropped by the six data quality gates and deduplicated to **189,952 customers** — exactly one row per customer in the SCD Type 1 silver table.
* 10,920-row gold aggregate (daily signups by city × account status, six measures) that fully reconciles to the 189,952 silver customers.

---

## 📬 Contact

**Naman Agarwal** — [GitHub: @NamanAgarwal20](https://github.com/NamanAgarwal20)

_This project demonstrates hands-on, production-style Databricks data engineering — from raw file ingestion to governed, orchestrated, quality-enforced lakehouse tables._
