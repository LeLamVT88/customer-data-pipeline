from src.utils.spark_utils import create_spark
from src.utils.config import CLEAN_PATH, DIM_PATH, FACT_PATH


def run(clean_input_path: str, dim_output_path: str, fact_output_path: str) -> None:
    spark = create_spark("Build-Dim-Fact")

    df = spark.read.parquet(clean_input_path)

    dim_customer = df.select(
        "customer_id",
        "gender",
        "age",
        "city_tier",
    )

    fact_customer_activity = df.select(
        "customer_id",
        "monthly_online_orders",
        "monthly_store_visits",
        "avg_online_spend",
        "avg_store_spend",
        "total_monthly_spend"
    )

    dim_customer.write.mode("overwrite").parquet(dim_output_path)
    fact_customer_activity.write.mode("overwrite").parquet(fact_output_path)

    print(f"[OK] Dimension written to: {dim_output_path}")
    print(f"[OK] Fact written to: {fact_output_path}")
    spark.stop()


if __name__ == "__main__":
    run(
        clean_input_path=CLEAN_PATH,
        dim_output_path=DIM_PATH,
        fact_output_path=FACT_PATH,
    )