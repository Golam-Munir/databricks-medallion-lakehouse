# silver_config.py
# Configuration for Silver layer transformations

SILVER_CONFIG = {
    "cust_info": {
        "bronze_table": "`databricks-medallion-lakehouse`.bronze.cust_info",
        "silver_table": "`databricks-medallion-lakehouse`.silver.cust_info",
        "primary_key": ["cst_id"],  # Deduplicate by this column
    },
    "prd_info": {
        "bronze_table": "`databricks-medallion-lakehouse`.bronze.prd_info",
        "silver_table": "`databricks-medallion-lakehouse`.silver.prd_info",
        "primary_key": ["prd_id"],
    },
    "sales_details": {
        "bronze_table": "`databricks-medallion-lakehouse`.bronze.sales_details",
        "silver_table": "`databricks-medallion-lakehouse`.silver.sales_details",
        "primary_key": ["order_number"],
    },
    "cust_az12": {
        "bronze_table": "`databricks-medallion-lakehouse`.bronze.cust_az12",
        "silver_table": "`databricks-medallion-lakehouse`.silver.cust_az12",
        "primary_key": ["customer_id"],
    },
    "loc_a101": {
        "bronze_table": "`databricks-medallion-lakehouse`.bronze.loc_a101",
        "silver_table": "`databricks-medallion-lakehouse`.silver.loc_a101",
        "primary_key": ["location_id"],
    },
    "px_cat_g1v2": {
        "bronze_table": "`databricks-medallion-lakehouse`.bronze.px_cat_g1v2",
        "silver_table": "`databricks-medallion-lakehouse`.silver.px_cat_g1v2",
        "primary_key": ["category_id"],
    },
}