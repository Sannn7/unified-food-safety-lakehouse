# Unified Multi-City Food Safety Data Lakehouse 🛡️

## Overview
An end-to-end Data Engineering pipeline that unifies fragmented food inspection data from **Chicago** and **Dallas**. It ingests raw JSON data into an **Azure Data Lake (Bronze)**, cleans and standardizes schemas using **PySpark (Silver)**, and applies a **Logic-Based NLP Classifier** to categorize violations (Gold).

The final output powers a **Power BI Dashboard** enabling stakeholders to track risk trends and active pest infestations in real-time.

## Architecture
**Tech Stack:** Azure Data Lake Gen2, Azure Databricks (PySpark), Python, Power BI.

1.  **Ingestion (Bronze):** Python scripts hit city APIs and load raw JSON into ADLS Gen2.
2.  **Transformation (Silver):** Databricks (PySpark) unifies schema mismatches (e.g., Unpivoting Dallas columns vs Parsing Chicago text).
3.  **Enrichment (Gold):** Custom NLP logic classifies violation text into risk categories (Pest, Hygiene, Temperature).
4.  **Visualization:** Power BI Dashboard.

## Dashboard
![Dashboard Preview](images/dashboard_screenshot.png)

## Project Structure
*   `src/`: Python scripts for API ingestion (Chicago & Dallas).
*   `notebooks/`: Databricks PySpark notebooks for Bronze->Silver and Silver->Gold transformation.

## 🔧 Key Features
*   **Schema Normalization:** converted "Wide" Dallas data to match "Long" Chicago format.
*   **Risk Classification:** Built a keyword-based NLP classifier to tag unstructured text.
*   **Medallion Architecture:** Strict separation of Raw, Clean, and Aggregated data.

## Impact
*   Reduced reporting latency from **7 days to Real-time**.
*   Identified **High-Risk Clusters** (Pest Infestations) using automated text analysis.