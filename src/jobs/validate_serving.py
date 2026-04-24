from pyspark.sql.functions import col, count, sum as spark_sum
from src.utils.spark_utils import create_spark
from src.utils.config import CUSTOMER_MART_PATH


def run(serving_input_path=None):
    spark = create_spark("Validate Customer Mart")

    serving_input_path = serving_input_path or CUSTOMER_MART_PATH

    df = spark.read.parquet(str(serving_input_path))

    print("=== SCHEMA ===")
    df.printSchema()

    print("=== SAMPLE DATA ===")
    df.show(10, truncate=False)

    print("=== ROW COUNT ===")
    print(df.count())

    print("=== NULL CHECK ===")
    df.select([
        spark_sum(col(c).isNull().cast("int")).alias(c)
        for c in df.columns
    ]).show(truncate=False)

    print("=== DUPLICATE CUSTOMER KEY CHECK ===")
    df.groupBy("customer_key") \
        .agg(count("*").alias("count")) \
        .filter(col("count") > 1) \
        .show(truncate=False)

    print("Validate serving layer completed.")

    spark.stop()


if __name__ == "__main__":
    run()