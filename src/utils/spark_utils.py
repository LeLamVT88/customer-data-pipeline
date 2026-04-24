try:
    from pyspark.sql import SparkSession
except Exception as e:  # pragma: no cover - helpful runtime message
    SparkSession = None
    _IMPORT_ERROR = e


def create_spark(app_name: str):
    if SparkSession is None:
        raise RuntimeError(
            "PySpark is not installed or failed to import. Install it with `pip install pyspark` "
            "or activate the project's virtualenv. Original error: {}".format(_IMPORT_ERROR)
        )

    return (
        SparkSession.builder
        .appName(app_name)
        .getOrCreate()
    )