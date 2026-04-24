from pyspark.sql.functions import col, lower
from src.utils.spark_utils import create_spark
from src.utils.config import (
    BRONZE_CUSTOMER_PATH,
    SILVER_CUSTOMER_PATH,
    BAD_RECORDS_PATH
)


def run(input_path=None, output_path=None, bad_path=None):
    spark = create_spark("Clean Customer Data")

    input_path = input_path or BRONZE_CUSTOMER_PATH
    output_path = output_path or SILVER_CUSTOMER_PATH
    bad_path = bad_path or BAD_RECORDS_PATH

    print(f"Reading bronze data from: {input_path}")

    df = spark.read.parquet(str(input_path))

    # ---- RULES ----
    df_clean = df.withColumn("gender", lower(col("gender")))

    df_valid = df_clean.filter(
    (col("age") > 0) &
    (col("monthly_income") >= 0) &
    (col("monthly_online_orders") >= 0) &
    (col("monthly_store_visits") >= 0) &
    (col("avg_online_spend") >= 0) &
    (col("avg_store_spend") >= 0)
)

    df_bad = df_clean.subtract(df_valid)

    print(f"Valid rows: {df_valid.count()}")
    print(f"Bad rows: {df_bad.count()}")

    # ---- WRITE ----
    df_valid.write.mode("overwrite").parquet(str(output_path))
    df_bad.write.mode("overwrite").parquet(str(bad_path))

    print("Clean data completed.")

    spark.stop()


if __name__ == "__main__":
    run()