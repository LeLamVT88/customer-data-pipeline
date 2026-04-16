from src.utils.spark_utils import create_spark
from src.utils.config import SERVING_PATH


def run(serving_input_path: str) -> None:
    spark = create_spark("Validate-Serving")

    df = spark.read.parquet(serving_input_path)

    print("\n=== SCHEMA ===")
    df.printSchema()

    print("\n=== SAMPLE DATA ===")
    df.show(5, truncate=False)

    print("\n=== ROW COUNT ===")
    print(df.count())

    print("\n=== NULL CHECK ===")
    df.selectExpr([
        f"sum(case when {c} is null then 1 else 0 end) as {c}"
        for c in df.columns
    ]).show(truncate=False)

    print("\n=== DESCRIBE total_monthly_spend ===")
    if "total_monthly_spend" in df.columns:
        df.select("total_monthly_spend").describe().show()
    else:
        print("Column total_monthly_spend not found.")

    spark.stop()


if __name__ == "__main__":
    run(
        serving_input_path=SERVING_PATH,
    )