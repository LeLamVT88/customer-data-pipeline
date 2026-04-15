from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum

SERVING_CUSTOMER_MART_PATH = "data/serving/customer_mart"


def main():
    spark = SparkSession.builder.appName("Validate Customer Mart").getOrCreate()

    df = spark.read.parquet(SERVING_CUSTOMER_MART_PATH)

    print("===== SCHEMA =====")
    df.printSchema()

    print("===== SAMPLE DATA =====")
    df.show(5, truncate=False)

    print("===== ROW COUNT =====")
    print(df.count())

    print("===== NULL CHECK =====")
    df.select([
        spark_sum(col(c).isNull().cast("int")).alias(c)
        for c in df.columns
    ]).show()

    if "total_monthly_spend" in df.columns:
        print("===== DESCRIBE total_monthly_spend =====")
        df.select("total_monthly_spend").describe().show()

    spark.stop()


if __name__ == "__main__":
    main()