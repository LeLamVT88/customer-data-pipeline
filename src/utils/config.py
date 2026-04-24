from pathlib import Path

# Project root
PROJECT_ROOT = Path(__file__).resolve().parents[2]

# Data lake layers
DATA_DIR = PROJECT_ROOT / "data"

RAW_PATH = DATA_DIR / "raw" / "customer_shopping_trends.csv"

BRONZE_DIR = DATA_DIR / "bronze"
SILVER_DIR = DATA_DIR / "silver"
GOLD_DIR = DATA_DIR / "gold"
SERVING_DIR = DATA_DIR / "serving"
QUARANTINE_DIR = DATA_DIR / "quarantine"

# Output datasets
BRONZE_CUSTOMER_PATH = BRONZE_DIR / "customer_raw"
SILVER_CUSTOMER_PATH = SILVER_DIR / "customer_clean"

DIM_CUSTOMER_PATH = GOLD_DIR / "dim_customer"
FACT_CUSTOMER_ACTIVITY_PATH = GOLD_DIR / "fact_customer_activity"

CUSTOMER_MART_PATH = SERVING_DIR / "customer_mart"

BAD_RECORDS_PATH = QUARANTINE_DIR / "bad_customer_records"
DATA_QUALITY_REPORT_PATH = SERVING_DIR / "data_quality_report"