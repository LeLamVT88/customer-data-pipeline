from src.utils.spark_utils import create_spark

def run(source_csv_path: str, raw_output_path: str) -> None:
    spark = create_spark("Ingest-Raw")

    df = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(source_csv_path)
    )

    (
        df.write
        .mode("overwrite")
        .parquet(raw_output_path)
    )

    spark.stop()