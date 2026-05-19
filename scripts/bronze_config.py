# -----------------------------------------------------
# databricks-medallion-lakehouse project by golam munir
# -----------------------------------------------------
# bronze_config.py
# Configuration file for Bronze Layer data sources
# This file defines where raw CSV files are located and what table names to create

BRONZE_CONFIG = {
    # CRM Source System
    "cust_info": "/Volumes/databricks-medallion-lakehouse/bronze/source_system/source_crm/cust_info.csv",
    "prd_info": "/Volumes/databricks-medallion-lakehouse/bronze/source_system/source_crm/prd_info.csv",
    "sales_details": "/Volumes/databricks-medallion-lakehouse/bronze/source_system/source_crm/sales_details.csv",
    
    # ERP Source System
    "cust_az12": "/Volumes/databricks-medallion-lakehouse/bronze/source_system/source_erp/CUST_AZ12.csv",
    "loc_a101": "/Volumes/databricks-medallion-lakehouse/bronze/source_system/source_erp/LOC_A101.csv",
    "px_cat_g1v2": "/Volumes/databricks-medallion-lakehouse/bronze/source_system/source_erp/PX_CAT_G1V2.csv",
}