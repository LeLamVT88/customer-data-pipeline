from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, trim, lower, upper, regexp_replace

RAW_PATH = "data/raw/Consumer_Shopping_Trends_2026.csv"
CLEAN_BASE_PATH = "data/clean/base"
DIM_CUSTOMER_PATH = "data/dim/dim_customer"
FACT_CUSTOMER_BEHAVIOR_PATH = "data/fact/fact_customer_behavior"
SERVING_CUSTOMER_MART_PATH = "data/serving/customer_mart"


def create_spark_session():
    return (
        SparkSession.builder
        .appName("Customer Data Pipeline")
        .getOrCreate()
    )


def extract(spark, path):
    return spark.read.csv(path, header=True, inferSchema=True)


def clean_data(df):
    df = df.dropDuplicates()

    # ví dụ clean realistic hơn
    string_cols = [c for c, t in df.dtypes if t == "string"]

    for c in string_cols:
        df = df.withColumn(c, trim(col(c)))

    if "gender" in df.columns:
        df = df.withColumn(
            "gender",
            when(lower(col("gender")).isin("male", "m"), "Male")
            .when(lower(col("gender")).isin("female", "f"), "Female")
            .otherwise("Unknown")
        )

    if "city_tier" in df.columns:
        df = df.withColumn(
            "city_tier",
            regexp_replace(col("city_tier"), r"\s+", "")
        )

    return df.dropna()


def add_customer_id(df):
    from pyspark.sql.functions import monotonically_increasing_id
    return df.withColumn("customer_id", monotonically_increasing_id())


def build_dim_customer(df):
    dim_cols = [
        "customer_id", "age", "gender", "city", "city_tier",
        "membership_type", "shopping_preference"
    ]
    existing_cols = [c for c in dim_cols if c in df.columns]
    return df.select(*existing_cols).dropDuplicates(["customer_id"])


def build_fact_customer_behavior(df):
    fact_cols = [
        "customer_id",
        "monthly_online_orders",
        "monthly_store_visits",
        "avg_online_spend",
        "avg_store_spend",
        "total_monthly_spend"
    ]
    existing_cols = [c for c in fact_cols if c in df.columns]
    return df.select(*existing_cols)


def build_serving_customer_mart(dim_customer, fact_customer_behavior):
    return dim_customer.join(fact_customer_behavior, on="customer_id", how="inner")


def feature_engineering(df):
    if all(c in df.columns for c in [
        "monthly_online_orders",
        "avg_online_spend",
        "monthly_store_visits",
        "avg_store_spend"
    ]):
        df = df.withColumn(
            "total_monthly_spend",
            col("monthly_online_orders") * col("avg_online_spend") +
            col("monthly_store_visits") * col("avg_store_spend")
        )
    return df


def load_parquet(df, path, mode="overwrite", partition_cols=None):
    writer = df.write.mode(mode)
    if partition_cols:
        writer = writer.partitionBy(*partition_cols)
    writer.parquet(path)


def main():
    spark = create_spark_session()

    raw_df = extract(spark, RAW_PATH)
    clean_df = clean_data(raw_df)
    base_df = add_customer_id(clean_df)
    base_df = feature_engineering(base_df)

    dim_customer = build_dim_customer(base_df)
    fact_customer_behavior = build_fact_customer_behavior(base_df)
    customer_mart = build_serving_customer_mart(dim_customer, fact_customer_behavior)

    load_parquet(base_df, CLEAN_BASE_PATH, mode="overwrite")
    load_parquet(dim_customer, DIM_CUSTOMER_PATH, mode="overwrite")
    load_parquet(fact_customer_behavior, FACT_CUSTOMER_BEHAVIOR_PATH, mode="overwrite")
    load_parquet(
        customer_mart,
        SERVING_CUSTOMER_MART_PATH,
        mode="overwrite",
        partition_cols=["shopping_preference"] if "shopping_preference" in customer_mart.columns else None
    )

    print("Pipeline completed successfully.")
    spark.stop()


if __name__ == "__main__":
    main()