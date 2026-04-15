from pyspark.sql.functions import col
from src.utils.spark_utils import create_spark

def run(dim_path: str, fact_path: str, serving_output_path: str) -> None:
    spark = create_spark("Build-Serving")

    dim_df = spark.read.parquet(dim_path)
    fact_df = spark.read.parquet(fact_path)

    customer_mart = dim_df.join(fact_df, on="customer_id", how="inner")

    customer_mart.write.mode("overwrite").parquet(serving_output_path)

    spark.stop()