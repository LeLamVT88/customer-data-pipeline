from pyspark.sql.functions import col, when, monotonically_increasing_id
from src.utils.spark_utils import create_spark

def run(raw_input_path: str, clean_output_path: str) -> None:
    spark = create_spark("Clean-Data")

    df = spark.read.parquet(raw_input_path)

    df_clean = (
        df.dropDuplicates()
          .dropna()
          .withColumn("customer_id", monotonically_increasing_id())
    )

    (
        df_clean.write
        .mode("overwrite")
        .parquet(clean_output_path)
    )

    spark.stop()