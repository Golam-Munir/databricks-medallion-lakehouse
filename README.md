# Databricks Medallion Lakehouse

![Databricks](https://img.shields.io/badge/Databricks-FF3621?style=for-the-badge&logo=databricks&logoColor=white)
![PySpark](https://img.shields.io/badge/Apache%20Spark-E25A1C?style=for-the-badge&logo=apachespark&logoColor=white)
![Delta Lake](https://img.shields.io/badge/Delta%20Lake-003366?style=for-the-badge&logo=delta&logoColor=white)
![Python](https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white)
![SQL](https://img.shields.io/badge/SQL-4479A1?style=for-the-badge&logo=mysql&logoColor=white)
![Status](https://img.shields.io/badge/Status-Complete-brightgreen?style=for-the-badge)

A production-grade data lakehouse built on Databricks using the **Medallion Architecture** (Bronze → Silver → Gold). Demonstrates end-to-end data engineering: ingestion, cleaning, dimensional modeling, and orchestration using PySpark, Delta Lake, and SQL.

> **Portfolio Project** — Built as part of a Data Engineering career transition. Designed for the Australian DE job market.

**Portfolio Goal:** Showcase cloud-native data engineering skills (Databricks, PySpark, Delta Lake, SQL) and dimensional data modelling for the Australian market.

---

## Problem & Context

Companies receive data from multiple source systems (CRM, ERP) in raw, unstructured formats. Without proper data engineering:
- Analysts repeat cleanup logic (inefficient, inconsistent)
- Bad data drives bad decisions
- Scaling becomes impossible

This project demonstrates the **industry-standard solution**: a structured lakehouse that ingests raw data, cleans it once, and models it for analytics.

---

## Solution Architecture

### Medallion Pattern (Bronze → Silver → Gold)

```
Raw CSV Files (CRM + ERP)
         │
         ▼
┌─────────────────────┐
│    BRONZE LAYER     │  Raw copy, no transformation
│  6 Delta Tables     │  Audit trail, compliance
│  18,494 - 60,398    │  Full reload on each run
└─────────────────────┘
         │
         ▼
┌─────────────────────┐
│    SILVER LAYER     │  Cleaned & standardized
│  6 Delta Tables     │  Trimmed, deduplicated
│  295 - 27,659 rows  │  Single source of truth
└─────────────────────┘
         │
         ▼
┌─────────────────────┐
│     GOLD LAYER      │  Business-ready star schema
│  3 Delta Tables     │  dim_customers, dim_products
│  295 - 27,659 rows  │  fact_sales
└─────────────────────┘
         │
         ▼
  Analytics / BI Tools
```

### Data Lineage

| Source | Bronze | Silver | Gold |
|--------|--------|--------|------|
| **CRM** | cust_info, prd_info, sales_details | Cleaned versions | dim_customers, dim_products, fact_sales |
| **ERP** | cust_az12, loc_a101, px_cat_g1v2 | Standardized | (Joined into dimensions) |

---

## Key Technical Decisions & Why

### 1. Delta Lake Format (Not Parquet)
**Why:** ACID transactions, schema validation, time-travel capability, transaction logs for audit trails.
**Impact:** Reliable data, recoverable from failures, auditable changes.

### 2. Table-Specific Silver Transformations (Not Generic Loop)
**Why:** Each source has unique quality issues (date conversions, NULL handling, deduplication logic).
**Impact:** Maintainable, scalable, shows understanding of real-world complexity.

### 3. Deduplication Strategy (Keep Latest Version)
**Why:** Products appear multiple times with different costs; kept latest by start_date (most relevant for current sales).
**Impact:** Accurate aggregations, single source of truth.

### 4. SQL for Gold Layer (Not Just PySpark)
**Why:** Business analysts must understand and validate dimensional modeling logic.
**Impact:** Transparency, maintainability, stakeholder confidence.

### 5. Left Joins in Fact Table
**Why:** Some sales have products/customers not in dimensions (data quality issues).
**Impact:** No lost transactions, issues visible in NULL counts.

---

## Data Quality Improvements (Bronze → Silver)

| Issue | Bronze | Silver | Fix Applied |
|-------|--------|--------|--------------|
| Extra spaces in names | ` Jon`, ` Torres` | Jon, Torres | trim() |
| Gender codes | M, F | Male, Female | Standardization |
| Marital status | M, S | Married, Single | Standardization |
| Duplicate products | 397 rows | 295 rows | Kept latest by date |
| NULL costs | 2 products | Marked as 0 | when() logic |
| Integer dates | 20101229 | 2010-12-29 | to_date() conversion |
| Duplicate customers | 18,494 | 18,485 | Deduplicate by ID |

---

## Project Structure
## Project Structure

```
databricks-medallion-lakehouse/
├── notebooks/
│   ├── bronze/
│   │   └── 01_bronze_load.ipynb              # Load 6 CSVs → Bronze Delta tables
│   ├── silver/
│   │   ├── 02a_silver_cust_info.ipynb        # Trim, standardize gender/marital codes
│   │   ├── 02b_silver_prd_info.ipynb         # Dedup products, handle NULL costs
│   │   ├── 02c_silver_sales_details.ipynb    # Convert integer dates → DATE type
│   │   └── 02d_silver_erp_tables.ipynb       # ERP tables (loop pattern)
│   └── gold/
│       └── 03_gold_model.ipynb               # Build star schema (dim + fact)
├── scripts/
│   ├── bronze_config.py                      # CSV file paths (config over code)
│   └── silver_config.py                      # Table transformation config
└── README.md
```

---

## How to Run

### Prerequisites
- Databricks Community Edition (free)
- Git sync folder connected to this GitHub repo

### Setup (One-Time)

1. **Create Catalog & Schemas**
```sql
   CREATE CATALOG IF NOT EXISTS databricks_medallion_lakehouse;
   CREATE SCHEMA IF NOT EXISTS `databricks-medallion-lakehouse`.bronze;
   CREATE SCHEMA IF NOT EXISTS `databricks-medallion-lakehouse`.silver;
   CREATE SCHEMA IF NOT EXISTS `databricks-medallion-lakehouse`.gold;
```

2. **Create Volume for CSVs**
   - In Databricks Catalog → `databricks-medallion-lakehouse` → bronze
   - Create Volume: `source_system`
   - Upload CSVs: `datasets/engineering/source_crm/` and `source_erp/`

3. **Clone This Repo to Databricks**
   - Workspace → Repo → Clone
   - GitHub URL: `https://github.com/Golam-Munir/databricks-medallion-lakehouse`
   - Choose path: `/Workspace/Users/{your-email}/databricks-medallion-lakehouse`

### Run the Pipeline

**Option 1: Run Notebooks Sequentially**

**Run Notebooks in This Order:**

```
Step 1:  notebooks/bronze/01_bronze_load.ipynb
Step 2:  notebooks/silver/02a_silver_cust_info.ipynb
Step 3:  notebooks/silver/02b_silver_prd_info.ipynb
Step 4:  notebooks/silver/02c_silver_sales_details.ipynb
Step 5:  notebooks/silver/02d_silver_erp_tables.ipynb
Step 6:  notebooks/gold/03_gold_model.ipynb
```


**Option 2: Create a Databricks Job (Production)**
- Create multi-task job with dependencies
- Schedule daily/weekly
- Set up monitoring & alerts

### Verify Success

```sql
-- Check row counts
SELECT COUNT(*) FROM `databricks-medallion-lakehouse`.gold.dim_customers;  -- 18,485
SELECT COUNT(*) FROM `databricks-medallion-lakehouse`.gold.dim_products;   -- 295
SELECT COUNT(*) FROM `databricks-medallion-lakehouse`.gold.fact_sales;     -- 27,659

-- Sample analytics query
SELECT 
    c.first_name,
    c.last_name,
    SUM(f.sales_amount) as total_sales,
    COUNT(*) as orders
FROM `databricks-medallion-lakehouse`.gold.fact_sales f
JOIN `databricks-medallion-lakehouse`.gold.dim_customers c 
    ON f.customer_key = c.customer_id
GROUP BY c.first_name, c.last_name
ORDER BY total_sales DESC
LIMIT 10;
```

---

## What I Learned

### Technical Insights

1. **Date Handling is Critical**
   - Source data had dates as integers (20101229)
   - Used `to_date(lpad(...), "yyyyMMdd")` for conversion
   - Invalid dates caught with `try_to_date()` for safety
   - Lesson: Type validation must happen early (Bronze layer)

2. **Deduplication Strategy Matters**
   - Same product appeared 3 times with different costs
   - Decision: Keep latest by start_date (most relevant for current analytics)
   - Real-world impact: Without this, revenue would be inflated by 102 duplicate rows

3. **String Cleaning is Underrated**
   - Extra spaces (` Jon` vs `Jon`) break joins
   - `trim()` on all string columns is non-negotiable
   - Lesson: Data quality compounds through layers

4. **Left Joins Preserve Data**
   - Some sales referenced products/customers not in source data
   - Using LEFT JOIN shows issues (NULL counts) instead of silently losing data
   - Lesson: Visibility of data quality issues > silent failures

### Data Engineering Insights

1. **One-Time Cleanup > Repeated Logic**
   - Silver layer trimmed spaces once
   - Every downstream query benefits
   - Without Silver, each analyst would trim independently

2. **Configuration Over Code**
   - Separate `bronze_config.py` from `01_bronze_load.ipynb`
   - Scales from 6 tables to 600 without code changes
   - Lesson: Parameterize early

3. **Table-Specific Transformations > Generic Patterns**
   - Tried generic loop first; failed
   - Each table had unique needs (date conversion, NULL handling, deduplication)
   - Lesson: Real data is messy; generic solutions can't handle it

---

## Metrics

| Metric | Value |
|--------|-------|
| **Total Rows Processed** | 117K (Bronze) |
| **Duplicates Removed** | 111 rows (0.09%) |
| **Data Quality Fixes** | 19 invalid dates, 2 NULL costs, 9 duplicate customers |
| **Tables Created** | 15 (6 Bronze + 6 Silver + 3 Gold) |
| **Transformation Complexity** | 5 types (trim, standardize, convert dates, deduplicate, join) |
| **Time to Complete** | ~8 hours (learning + building) |

---

## Future Improvements

1. **Incremental Loading**
   - Current: Full load (overwrite)
   - Next: Append/Merge for daily updates
   - Would need: Change Data Capture (CDC) or timestamp-based deltas

2. **Data Quality Monitoring**
   - Add Great Expectations or dbt tests
   - Monitor NULL counts, duplicate rates, data freshness
   - Alert on anomalies

3. **Slowly Changing Dimensions (SCD)**
   - Current: Type 1 (overwrite)
   - Next: Type 2 (track history with dates)
   - Would enable "customer history as of date X"

4. **Aggregations in Gold**
   - Pre-computed summaries (sales by month, customer, product)
   - Dramatic query performance improvement
   - Trade-off: Storage vs. query speed

---

## Resources & Tools

- **Databricks Community Edition**: Free tier for learning
- **PySpark Functions**: `trim()`, `when()`, `to_date()`, `dropDuplicates()`, `join()`
- **Delta Lake**: ACID transactions, schema validation, time-travel
- **Unity Catalog**: Governance & data discovery (future)
- **dbt** (Optional): Data transformation orchestration & testing

---

## Contact & Questions

- **Author**: Golam Munir
- **Email**: golam.mt.munir@gmail.com
- **GitHub**: github.com/Golam-Munir
- **Location**: Melbourne, Australia

---

**Last Updated**: May 2026  
**Status**: ✅ Complete (Bronze → Silver → Gold)
