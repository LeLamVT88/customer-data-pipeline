from src.utils.spark_utils import create_spark
from src.utils.config import SOURCE_CSV_PATH, RAW_PATH

def run(source_csv_path: str, raw_output_path: str) -> None:
    spark = create_spark("Ingest-Raw")

    df = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(source_csv_path)
    )

    df.write.mode("overwrite").parquet(raw_output_path)

    print(f"[OK] Raw data written to: {raw_output_path}")
    spark.stop()


if __name__ == "__main__":
    run(
        source_csv_path=SOURCE_CSV_PATH,
        raw_output_path=RAW_PATH,
    )