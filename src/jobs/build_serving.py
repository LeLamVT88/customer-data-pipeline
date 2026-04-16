from src.utils.spark_utils import create_spark
from src.utils.config import DIM_PATH, FACT_PATH, SERVING_PATH


def run(dim_input_path: str, fact_input_path: str, serving_output_path: str) -> None:
    spark = create_spark("Build-Serving")

    dim_customer = spark.read.parquet(dim_input_path)
    fact_customer_activity = spark.read.parquet(fact_input_path)

    customer_mart = dim_customer.join(
        fact_customer_activity,
        on="customer_id",
        how="inner"
    )

    customer_mart.write.mode("overwrite").parquet(serving_output_path)

    print(f"[OK] Serving layer written to: {serving_output_path}")
    spark.stop()


if __name__ == "__main__":
    run(
        dim_input_path=DIM_PATH,
        fact_input_path=FACT_PATH,
        serving_output_path=SERVING_PATH,
    )