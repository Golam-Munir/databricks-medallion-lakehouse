📐 # Technical Architecture 

## System Design

### Databricks Environment

```
Databricks Community Edition
├── Catalogue: databricks-medallion-lakehouse
│   ├── Schema: bronze
│   │   ├── Volume: source_system (6 CSV files)
│   │   └── Tables: cust_info, prd_info, sales_details, cust_az12, loc_a101, px_cat_g1v2
│   ├── Schema: silver
│   │   └── Tables: cust_info, prd_info, sales_details, cust_az12, loc_a101, px_cat_g1v2
│   └── Schema: gold
│       ├── Table: dim_customers (18,485 rows)
│       ├── Table: dim_products (295 rows)
│       └── Table: fact_sales (27,659 rows)
└── Git Integration: Auto-sync with GitHub
```

### Data Flow

```
CSV Files (Volumes)
    ↓ [PySpark: spark.read.csv()]
Bronze Delta Tables (Raw, no transforms)
    ↓ [PySpark: trim, when, dropDuplicates]
Silver Delta Tables (Clean, standardised)
    ↓ [Spark SQL: joins, aggregations]
Gold Delta Tables (Star schema, business-ready)
    ↓ [SQL: SELECT * FROM gold.dim_customers]
Analytics / BI Tools
```

---

## Transformation Logic

### Bronze Layer (Load)

**Pattern**: CSV → PySpark DataFrame → Delta Table

```python
# Pseudocode
for each CSV file:
    df = spark.read.csv(path, header=true, inferSchema=true)
    df.write.mode("overwrite").format("delta").saveAsTable(...)
```

**Key Options**:
- `header=true`: First row is column names
- `inferSchema=true`: Spark infers data types automatically
- `mode=overwrite`: Replace if table exists (safe to re-run)

**Why Delta (not Parquet)**:
- ACID guarantees (all-or-nothing writes)
- Schema validation
- Time travel (query old versions)
- Transaction logs for auditing

---

### Silver Layer (Clean)

**Pattern**: Read Bronze → Apply Transformations → Write Silver

#### Transformation Types

| Transformation | Code | Example | Impact |
|---|---|---|---|
| **Trim** | `trim(col(...))` | ` Jon` → `Jon` | Accurate joins |
| **Standardize** | `when().otherwise()` | M → Male | Consistent reports |
| **Convert Date** | `to_date(lpad(...), "yyyyMMdd")` | 20101229 → 2010-12-29 | Date queries work |
| **Deduplicate** | `dropDuplicates([key])` | Keep first per ID | No inflated metrics |
| **Handle NULLs** | `when(col().isNull(), value)` | NULL → 0 | Usable data |

#### Example: cust_info Cleaning

```python
from pyspark.sql.functions import trim, col, when

# Read Bronze
df = spark.read.table("`databricks-medallion-lakehouse`.bronze.cust_info")

# Step 1: Trim spaces from all string columns
df_clean = df.select([
    trim(col(c)).alias(c) if df.schema[c].dataType.simpleString() == "string" else col(c)
    for c in df.columns
])

# Step 2: Standardise gender codes
df_clean = df_clean.withColumn(
    "cst_gndr",
    when(col("cst_gndr") == "M", "Male")
    .when(col("cst_gndr") == "F", "Female")
    .otherwise("Unknown")
)

# Step 3: Deduplicate by primary key
df_clean = df_clean.dropDuplicates(["customer_id"])

# Step 4: Write to Silver
df_clean.write.mode("overwrite").format("delta").saveAsTable(...)
```

---

### Gold Layer (Model)

**Pattern**: Read Silver → Join Dimensions → Star Schema → Write Gold

#### Dimensional Model (Star Schema)

```
          ┌─────────────────────────┐
          │     dim_customers        │
          │  customer_id (PK)        │
          │  first_name, last_name   │
          │  gender, marital_status  │
          │  country, birthdate      │
          └────────────┬────────────┘
                       │ FK
          ┌────────────▼────────────┐
          │       fact_sales         │
          │  order_number (PK)       │
          │  customer_key (FK)       │
          │  product_key (FK)        │
          │  order_date              │
          │  sales_amount            │
          │  quantity, price         │
          └────────────┬────────────┘
                       │ FK
          ┌────────────▼────────────┐
          │      dim_products        │
          │  product_id (PK)         │
          │  product_name, cost      │
          │  category, subcategory   │
          │  product_line            │
          └─────────────────────────┘
```

#### Example: Build fact_sales

```python
# Read Silver & Gold tables
sales = spark.read.table("`databricks-medallion-lakehouse`.silver.sales_details")
dim_cust = spark.read.table("`databricks-medallion-lakehouse`.gold.dim_customers")
dim_prod = spark.read.table("`databricks-medallion-lakehouse`.gold.dim_products")

# Join to get dimension keys
fact_sales = sales \
    .join(dim_cust, sales["customer_id"] == dim_cust["customer_id"], "left") \
    .join(dim_prod, sales["product_key"] == dim_prod["product_key"], "left") \
    .select(
        sales["order_number"],
        dim_cust["customer_id"].alias("customer_key"),
        dim_prod["product_id"].alias("product_key"),
        sales["order_date"],
        sales["sales_amount"],
        sales["quantity"]
    )

fact_sales.write.mode("overwrite").format("delta").saveAsTable(...)
```

**Why Left Joins**: Preserves all sales even if customer/product is missing. Shows data quality issues rather than silently dropping rows.

---

## Performance Characteristics

### Current State

| Table | Rows | Columns | Format | Partitioning |
|---|---|---|---|---|
| fact_sales | 27,659 | 9 | Delta | None |
| dim_customers | 18,485 | 9 | Delta | None |
| dim_products | 295 | 10 | Delta | None |

### Query Performance

```sql
-- Simple aggregation (fast, < 1s)
SELECT COUNT(*) FROM gold.fact_sales;

-- Fact + Dimension join (1-2s)
SELECT c.gender, COUNT(*)
FROM gold.fact_sales f
JOIN gold.dim_customers c ON f.customer_key = c.customer_id
GROUP BY c.gender;

-- Multi-table join (2-3s)
SELECT p.category, SUM(f.sales_amount)
FROM gold.fact_sales f
JOIN gold.dim_customers c ON f.customer_key = c.customer_id
JOIN gold.dim_products p ON f.product_key = p.product_id
GROUP BY p.category;
```

### Future Optimizations

1. **Partitioning by Date** — Queries on date ranges 10x faster
```python
df.write.partitionBy("order_date").mode("overwrite").format("delta").saveAsTable(...)
```

2. **Z-Ordering** — Join performance +50%
```sql
OPTIMIZE gold.fact_sales ZORDER BY (customer_key, product_key)
```

3. **Pre-aggregations** — Dashboard queries 100x faster
```sql
CREATE TABLE gold.agg_sales_monthly AS
SELECT DATE_TRUNC('month', order_date) as month, SUM(sales_amount) as total_sales
FROM gold.fact_sales GROUP BY 1;
```

---

## Error Handling & Data Quality

### Known Issues & Resolutions

| Issue | Bronze | Silver | Resolution |
|---|---|---|---|
| Invalid dates (00000000) | 19 occurrences | → NULL | `try_to_date()` instead of `to_date()` |
| NULL product costs | 2 products | → 0 | `when(col().isNull(), 0)` |
| Duplicate products | 102 rows | Removed | Keep latest by `prd_start_dt` |
| Mismatched ID formats | ERP strings (NASAW00011000) | Extracted integer | `substring() + cast(IntegerType())` |

### Monitoring Pattern (Recommended for Production)

```python
# Add after each transformation layer
def data_quality_check(df, table_name):
    print(f"\nData Quality: {table_name}")
    print(f"  Total rows: {df.count():,}")
    for c in df.columns:
        null_count = df.filter(col(c).isNull()).count()
        if null_count > 0:
            print(f"  NULLs in {c}: {null_count:,}")
```

---

## Lessons & Trade-offs

| Decision | Why | Trade-off |
|---|---|---|
| **Left Joins** | Keep all data, show quality issues | Some NULLs in result |
| **SCD Type 1** (overwrite) | Simpler, faster | Can't track historical changes |
| **No Pre-aggregations** | Flexibility, re-use facts | Slightly slower dashboard queries |
| **PySpark for Silver** | Flexible, programmatic | Harder for non-engineers to modify |
| **SQL for Gold** | Business-readable, auditable | Less programmatic flexibility |
| **Config files** | Separation of concerns, scalable | One extra file to maintain |

---

## Next Steps for Production

1. **Incremental Loading** — MERGE (upsert) instead of full OVERWRITE
2. **Orchestration** — Databricks Jobs with Bronze → Silver → Gold dependencies
3. **Data Quality Monitoring** — Automated NULL/duplicate checks, SLA alerts
4. **Slowly Changing Dimensions** — Type 2 SCD to track customer/product history
5. **Access Control** — Row-level security, column masking via Unity Catalogue
6. **CI/CD Pipeline** — Automated testing on pull requests before merge
