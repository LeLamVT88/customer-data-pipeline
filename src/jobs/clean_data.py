from pyspark.sql.functions import col, monotonically_increasing_id
from src.utils.spark_utils import create_spark
from src.utils.config import RAW_PATH, CLEAN_PATH


def run(raw_input_path: str, clean_output_path: str) -> None:
    spark = create_spark("Clean-Data")

    df = spark.read.parquet(raw_input_path)

    df_clean = (
        df.dropDuplicates()
          .dropna()
          .withColumn("customer_id", monotonically_increasing_id())
          .withColumn(
              "total_monthly_spend",
              col("monthly_online_orders") * col("avg_online_spend")
              + col("monthly_store_visits") * col("avg_store_spend")
          )
    )

    df_clean.write.mode("overwrite").parquet(clean_output_path)

    print(f"[OK] Clean data written to: {clean_output_path}")
    spark.stop()


if __name__ == "__main__":
    run(
        raw_input_path=RAW_PATH,
        clean_output_path=CLEAN_PATH,
    )