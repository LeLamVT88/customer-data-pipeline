from src.utils.spark_utils import create_spark
from src.utils.config import (
    DIM_CUSTOMER_PATH,
    FACT_CUSTOMER_ACTIVITY_PATH,
    CUSTOMER_MART_PATH
)


def run(dim_input_path=None, fact_input_path=None, serving_output_path=None):
    spark = create_spark("Build Customer Mart")

    dim_input_path = dim_input_path or DIM_CUSTOMER_PATH
    fact_input_path = fact_input_path or FACT_CUSTOMER_ACTIVITY_PATH
    serving_output_path = serving_output_path or CUSTOMER_MART_PATH

    dim_customer = spark.read.parquet(str(dim_input_path))
    fact_customer_activity = spark.read.parquet(str(fact_input_path))

    customer_mart = fact_customer_activity.join(
        dim_customer,
        on="customer_key",
        how="left"
    )

    customer_mart.write.mode("overwrite").parquet(str(serving_output_path))

    print("Build serving layer completed.")

    spark.stop()


if __name__ == "__main__":
    run()