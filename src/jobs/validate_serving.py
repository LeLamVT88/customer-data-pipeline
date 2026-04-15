from pyspark.sql.functions import col, sum
from src.utils.spark_utils import create_spark

def run(serving_path: str) -> None:
    spark = create_spark("Validate-Serving")

    df = spark.read.parquet(serving_path)

    print("row_count =", df.count())
    df.printSchema()
    df.show(5, truncate=False)

    df.select([
        sum(col(c).isNull().cast("int")).alias(c)
        for c in df.columns
    ]).show()

    spark.stop()