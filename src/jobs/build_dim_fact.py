from pyspark.sql.functions import monotonically_increasing_id
from src.utils.spark_utils import create_spark
from src.utils.config import (
    SILVER_CUSTOMER_PATH,
    DIM_CUSTOMER_PATH,
    FACT_CUSTOMER_ACTIVITY_PATH
)


def run(input_path=None, dim_output_path=None, fact_output_path=None):
    spark = create_spark("Build Dim and Fact")

    input_path = input_path or SILVER_CUSTOMER_PATH
    dim_output_path = dim_output_path or DIM_CUSTOMER_PATH
    fact_output_path = fact_output_path or FACT_CUSTOMER_ACTIVITY_PATH

    df = spark.read.parquet(str(input_path))

    dim_customer = df.select(
        "age",
        "gender",
        "city_tier",
        "shopping_preference"
    ).dropDuplicates().withColumn(
        "customer_key",
        monotonically_increasing_id()
    )

    fact_customer_activity = df.join(
        dim_customer,
        on=["age", "gender", "city_tier", "shopping_preference"],
        how="left"
    ).select(
        "customer_key",
        "monthly_income",
        "daily_internet_hours",
        "smartphone_usage_years",
        "social_media_hours",
        "online_payment_trust_score",
        "tech_savvy_score",
        "monthly_online_orders",
        "monthly_store_visits",
        "avg_online_spend",
        "avg_store_spend",
        "discount_sensitivity",
        "return_frequency",
        "avg_delivery_days",
        "delivery_fee_sensitivity",
        "free_return_importance",
        "product_availability_online",
        "impulse_buying_score",
        "need_touch_feel_score",
        "brand_loyalty_score",
        "environmental_awareness",
        "time_pressure_level"
    )

    dim_customer.write.mode("overwrite").parquet(str(dim_output_path))
    fact_customer_activity.write.mode("overwrite").parquet(str(fact_output_path))

    print("Build dim and fact completed.")

    spark.stop()


if __name__ == "__main__":
    run()