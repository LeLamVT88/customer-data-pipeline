import os
import sys
from pyspark.sql import SparkSession
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

def print_partition_info(df, name):
    print(f"\n=== {name} ===")
    print("Number of partitions:", df.rdd.getNumPartitions())

    sizes = df.rdd.mapPartitions(lambda it: [sum(1 for _ in it)]).collect()
    for idx, size in enumerate(sizes):
        print(f"Partition {idx}: {size} records")

def main():
    spark = SparkSession.builder \
        .appName("Check Partition") \
        .getOrCreate()

    # Load data
    df = spark.read.parquet("data/serving/customer_mart")

    # 1. Check current partitions
    print_partition_info(df, "ORIGINAL DATAFRAME")

    # 2. Explain plan for a common aggregation
    print("\n=== EXPLAIN PLAN ===")
    df.groupBy("city_tier").count().explain(True)

    # 3. Repartition by business column
    df_repartitioned = df.repartition(4)
    print_partition_info(df_repartitioned, "AFTER REPARTITION(4)")

    # 4. Coalesce before writing to reduce small files
    df_final = df_repartitioned.coalesce(2)
    print_partition_info(df_final, "AFTER COALESCE(2)")

    # 5. Write output partitioned by city_tier
    df_final.write \
        .mode("overwrite") \
        .partitionBy("city_tier") \
        .parquet("data/output/task3_customer_mart_partitioned")

    spark.stop()

if __name__ == "__main__":
    main()