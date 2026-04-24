from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Check_Validate_Serving").getOrCreate()

# Load serving data
df = spark.read.parquet("data/serving/customer_mart")

df.show(5)
df.printSchema()
df.count()

print("count =", df.count())

from pyspark.sql.functions import col, sum

# check null

df.select([
    sum(col(c).isNull().cast("int")).alias(c)
    for c in df.columns
]).show()

# thống kê spend
df.select("total_monthly_spend").describe().show()
df.select("total_monthly_spend").orderBy("total_monthly_spend", ascending=False).show(10)
spark.stop()