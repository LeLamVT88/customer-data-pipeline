from pyspark.sql.functions import col
from src.utils.spark_utils import create_spark

def run(clean_input_path: str, dim_output_path: str, fact_output_path: str) -> None:
    spark = create_spark("Build-Dim-Fact")

    df = spark.read.parquet(clean_input_path)

    dim_customer = df.select(
        "customer_id",
        "gender",
        "age",
        "city",
        "membership_type"
    ).dropDuplicates(["customer_id"])

    fact_spending = df.select(
        "customer_id",
        "monthly_online_orders",
        "monthly_store_visits",
        "avg_online_spend",
        "avg_store_spend",
        "total_monthly_spend"
    )

    dim_customer.write.mode("overwrite").parquet(dim_output_path)
    fact_spending.write.mode("overwrite").parquet(fact_output_path)

    spark.stop()