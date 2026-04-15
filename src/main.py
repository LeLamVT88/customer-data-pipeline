from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, when
spark = SparkSession.builder \
    .appName("Customer Data Pipeline") \
    .getOrCreate()

df = spark.read.csv(
    "data/Consumer_Shopping_Trends_2026.csv",           # đường dẫn file input
    header=True,                                        # dòng đầu tiên là tên cột
    inferSchema=True                                    # để Spark tự suy luận kiểu dữ liệu
)

df.printSchema()                                        # kiểm tra kiểu dữ liệu từng cột

# 4. CHECK NULL VALUES

null_counts = df.select(                                # tạo dataframe chứa số null theo từng cột
    [
        count(when(col(c).isNull(), c)).alias(c)        # nếu cột c là null thì đếm, đặt tên output = tên cột
        for c in df.columns                             # lặp qua tất cả cột trong dataframe
    ]
)

null_counts.show(truncate=False)                        # in kết quả check null dạng ngang
null_counts.show(vertical=True, truncate=False)         # in kết quả check null dạng dọc cho dễ nhìn

# 5. CHECK DUPLICATE ROWS

duplicate_df = (df.groupBy(df.columns)                 # group theo toàn bộ cột để kiểm tra row trùng hoàn toàn
.count()                                         # đếm số lần xuất hiện của mỗi row
    .filter(col("count") > 1)    )                       # chỉ giữ những row xuất hiện > 1 lần

duplicate_df.show(truncate=False)                       # in ra các row bị duplicate nếu có


# 6. CLEAN DATA

df_clean = df.dropDuplicates().dropna()                 # bỏ row trùng và bỏ row có null

# 7. FEATURE ENGINEERING

df_transformed = df_clean.withColumn(
    "total_monthly_spend",                              # tạo cột mới tên total_monthly_spend
    col("monthly_online_orders") * col("avg_online_spend") +    # tiền chi online mỗi tháng
    col("monthly_store_visits") * col("avg_store_spend")        # cộng với tiền chi tại cửa hàng
)

# 8. KIỂM TRA KẾT QUẢ TRANSFORM

df_transformed.select(                                  # chỉ chọn vài cột quan trọng để xem nhanh
    "monthly_online_orders",
    "avg_online_spend",
    "monthly_store_visits",
    "avg_store_spend",
    "total_monthly_spend"
).show(5, truncate=False)                               # show 5 dòng đầu để kiểm tra

# 9. TẠO FACT TABLE

fact_df = df_transformed.select(
    "monthly_online_orders",                            # số đơn online mỗi tháng
    "monthly_store_visits",                             # số lần ghé cửa hàng mỗi tháng
    "total_monthly_spend"                               # tổng chi tiêu mỗi tháng
)


# 10. TẠO DIMENSION TABLE

dim_df = df_transformed.select(
    "gender",                                           # giới tính
    "city_tier",                                        # loại thành phố
    "shopping_preference"                               # sở thích mua sắm
).distinct()                                            # chỉ giữ các tổ hợp duy nhất


# 11. KIỂM TRA FACT / DIM

fact_df.show(5, truncate=False)                         # xem trước fact table
dim_df.show(5, truncate=False)                          # xem trước dim table


# 12. GHI RA PARQUET

fact_df.write.mode("overwrite").parquet("output/fact_customer")  # ghi fact table ra parquet
dim_df.write.mode("overwrite").parquet("output/dim_customer")    # ghi dim table ra parquet

# 13. THÔNG BÁO HOÀN TẤT

print("Pipeline completed successfully.")               # báo chạy xong