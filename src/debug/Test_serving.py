from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import time

SERVING_PATH = "data/serving/customer_mart"

def create_spark():
    return SparkSession.builder \
        .appName("Task4-Serving-Layer-Validation") \
        .getOrCreate()

def benchmark_query(name, df_query):
    start = time.time()
    rows = df_query.count()
    end = time.time()
    print(f"{name}: rows={rows}, time={end - start:.4f} sec")

def main():
    spark = create_spark()

    df = spark.read.parquet(SERVING_PATH)

    print("\n=== 1. BASIC CHECK ===")
    df.printSchema()
    df.show(5, truncate=False)
    print("total rows =", df.count())

    print("\n=== 2. COLUMN + DTYPE CHECK ===")
    print(df.columns)
    for c, t in df.dtypes:
        print(f"{c}: {t}")

    print("\n=== 3. NULL CHECK ===")
    critical_cols = [
        "customer_id",
        "gender",
        "city_tier",
        "shopping_preference",
        "monthly_online_orders",
        "monthly_store_visits",
        "total_monthly_spend"
    ]

    df.select([
        sum(col(c).isNull().cast("int")).alias(c)
        for c in critical_cols
    ]).show()

    print("\n=== 4. DUPLICATE CHECK ===")
    dup_df = df.groupBy("customer_id").count().filter(col("count") > 1)
    dup_count = dup_df.count()
    print("duplicate customer_id count =", dup_count)
    if dup_count > 0:
        dup_df.show(10, truncate=False)

    print("\n=== 5. BUSINESS RULE CHECK ===")
    bad_df = df.filter(
        (col("monthly_online_orders") < 0) |
        (col("monthly_store_visits") < 0) |
        (col("total_monthly_spend") < 0)
    )
    print("bad rows =", bad_df.count())

    print("\n=== 6. DESCRIBE METRICS ===")
    df.select(
        "monthly_online_orders",
        "monthly_store_visits",
        "total_monthly_spend"
    ).describe().show()

    print("\n=== 7. TEST QUERIES ===")
    q1 = df.select("customer_id", "gender", "city_tier", "total_monthly_spend") \
           .orderBy(col("total_monthly_spend").desc())
    q1.show(10, truncate=False)

    q2 = df.groupBy("gender") \
           .agg(
               count("*").alias("num_customers"),
               round(sum("total_monthly_spend"), 2).alias("total_spend"),
               round(avg("total_monthly_spend"), 2).alias("avg_spend")
           ) \
           .orderBy(col("total_spend").desc())
    q2.show(truncate=False)

    q3 = df.groupBy("city_tier") \
           .agg(
               count("*").alias("num_customers"),
               round(sum("total_monthly_spend"), 2).alias("total_spend"),
               round(avg("total_monthly_spend"), 2).alias("avg_spend")
           ) \
           .orderBy(col("total_spend").desc())
    q3.show(truncate=False)

    q4 = df.groupBy("shopping_preference") \
           .agg(
               count("*").alias("num_customers"),
               round(sum("total_monthly_spend"), 2).alias("total_spend"),
               round(avg("total_monthly_spend"), 2).alias("avg_spend")
           ) \
           .orderBy(col("total_spend").desc())
    q4.show(truncate=False)

    seg_df = df.withColumn(
        "customer_segment",
        when(col("total_monthly_spend") >= 1000, "High Value")
        .when(col("total_monthly_spend") >= 500, "Medium Value")
        .otherwise("Low Value")
    )

    q5 = seg_df.groupBy("customer_segment") \
        .agg(
            count("*").alias("num_customers"),
            round(avg("total_monthly_spend"), 2).alias("avg_spend")
        ) \
        .orderBy(col("avg_spend").desc())
    q5.show(truncate=False)

    print("\n=== 8. EXPLAIN PLAN ===")
    q4.explain(True)

    print("\n=== 9. BENCHMARK ===")
    benchmark_query("Q1 Top customers", q1)
    benchmark_query("Q2 Spend by gender", q2)
    benchmark_query("Q3 Spend by city tier", q3)
    benchmark_query("Q4 Spend by shopping preference", q4)
    benchmark_query("Q5 Customer segment", q5)

    print("\n=== 10. CACHE TEST ===")
    df.cache()

    start1 = time.time()
    df.count()
    end1 = time.time()

    start2 = time.time()
    df.count()
    end2 = time.time()

    print("first count  =", round(end1 - start1, 4), "sec")
    print("second count =", round(end2 - start2, 4), "sec")

    print("\n=== 11. PROJECTION TEST ===")
    start3 = time.time()
    df.count()
    end3 = time.time()

    start4 = time.time()
    df.select("customer_id", "total_monthly_spend").count()
    end4 = time.time()

    print("full df count time      =", round(end3 - start3, 4), "sec")
    print("selected cols count time=", round(end4 - start4, 4), "sec")

    spark.stop()

if __name__ == "__main__":
    main()