from src.utils.spark_utils import create_spark
from src.utils.config import RAW_PATH, BRONZE_CUSTOMER_PATH


def run(raw_input_path=None, bronze_output_path=None):
    spark = create_spark("Ingest Raw Customer Data")

    raw_input_path = raw_input_path or RAW_PATH
    bronze_output_path = bronze_output_path or BRONZE_CUSTOMER_PATH

    print(f"Reading raw data from: {raw_input_path}")
    print(f"Writing bronze data to: {bronze_output_path}")

    df = spark.read.csv(
        str(raw_input_path),
        header=True,
        inferSchema=True
    )

    df.write.mode("overwrite").parquet(str(bronze_output_path))

    print("Ingest raw completed successfully.")

    spark.stop()


if __name__ == "__main__":
    run()