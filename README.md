# 🚀 Customer Data Pipeline (PySpark)

## 📖 Tổng quan

Đây là project xây dựng **pipeline xử lý dữ liệu khách hàng** sử dụng **PySpark** và **Airflow**, mô phỏng quy trình Data Engineering trong thực tế.

Pipeline này thực hiện:

* Xử lý dữ liệu thô (raw data)
* Làm sạch dữ liệu
* Biến đổi dữ liệu
* Xây dựng mô hình dữ liệu (fact & dimension)
* Tạo layer phục vụ phân tích (serving layer)

👉 Mục tiêu: mô phỏng cách Data Engineer xây dựng hệ thống dữ liệu phục vụ BI/Analytics.

---

## 🏗️ Kiến trúc Pipeline

```text
RAW DATA (CSV)
      ↓
[Extract] - ingest_raw.py
      ↓
[Clean + Validate] - clean_data.py
      ↓
[Transform] - build_dim_fact.py
  - Thêm customer_id
  - Feature engineering
      ↓
[Data Modeling]
  - Dim table
  - Fact table
      ↓
[Serving Layer] - build_serving.py
  - customer_mart (Parquet)
      ↓
[Validate] - validate_serving.py
```

Pipeline tuân theo **Data Engineering Lifecycle**:

* Data Generation
* Storage
* Ingestion
* Transformation
* Serving

---

## ⚙️ Công nghệ sử dụng

* Python
* PySpark
* Parquet (lưu trữ dữ liệu)
* Airflow (orchestration)
* Git (quản lý version)

---

## 📂 Cấu trúc project

```
customer-data-pipeline/
│
├── airflow/
│   └── dags/
│       └── Customer_Pipeline_dags.py  # Airflow DAG để orchestrate pipeline
│
├── data/
│   ├── raw/                          # Dữ liệu thô (CSV)
│   │   └── Consumer_Shopping_Trends_2026.csv
│   ├── clean/                        # Dữ liệu đã làm sạch
│   │   ├── base/                     # Dữ liệu base sau clean
│   │   ├── customer_data/            # Dữ liệu khách hàng
│   │   └── customers_clean/          # Dữ liệu khách hàng sạch
│   ├── dim/                          # Dimension tables
│   │   └── dim_customer/
│   ├── fact/                         # Fact tables
│   │   └── fact_customer_behavior/
│   ├── output/                       # Output partitioned
│   │   ├── customer_mart_partitioned/
│   │   └── task3_customer_mart_partitioned/
│   └── serving/                      # Serving layer
│       ├── customer_mart/
│       ├── dim_customer/
│       ├── fact_customer_activity/
│       └── spend_by_segment/
│
├── src/
│   ├── Pipeline.py                   # Module chính với functions
│   ├── jobs/                         # Các job riêng biệt
│   │   ├── ingest_raw.py
│   │   ├── clean_data.py
│   │   ├── build_dim_fact.py
│   │   ├── build_serving.py
│   │   └── validate_serving.py
│   ├── utils/                        # Utilities
│   │   ├── config.py
│   │   └── spark_utils.py
│   └── debug/                        # Debug scripts
│       ├── Check_Partition.py
│       ├── Check_Validate_Serving.py
│       └── Test_serving.py
│
├── LICENSE
└── README.md
```

---

## 🔄 Tiến độ triển khai

Dự án đã được triển khai hoàn chỉnh với các bước sau:

### ✅ 1. Extract (Ingest Raw Data)
- **Job**: `src/jobs/ingest_raw.py`
- **Mô tả**: Đọc dữ liệu CSV từ `data/raw/Consumer_Shopping_Trends_2026.csv` vào Spark DataFrame, lưu vào `data/raw/customer_data/` và `data/raw/customers_raw/`.
- **Trạng thái**: Hoàn thành (có file _SUCCESS)

### ✅ 2. Clean + Validate
- **Job**: `src/jobs/clean_data.py`
- **Mô tả**: Làm sạch dữ liệu: loại bỏ trùng lặp, trim strings, chuẩn hóa gender và city_tier, loại bỏ null. Lưu vào `data/clean/base/`, `data/clean/customer_data/`, `data/clean/customers_clean/`.
- **Trạng thái**: Hoàn thành (có file _SUCCESS)

### ✅ 3. Transform & Data Modeling
- **Job**: `src/jobs/build_dim_fact.py`
- **Mô tả**: Thêm customer_id, xây dựng dim_customer và fact_customer_behavior. Lưu vào `data/dim/dim_customer/` và `data/fact/fact_customer_behavior/`.
- **Trạng thái**: Hoàn thành (có file _SUCCESS)

### ✅ 4. Serving Layer
- **Job**: `src/jobs/build_serving.py`
- **Mô tả**: Xây dựng customer_mart partitioned theo city_tier, và các bảng serving khác. Lưu vào `data/serving/` và `data/output/`.
- **Trạng thái**: Hoàn thành (có file _SUCCESS, partitioned theo city_tier)

### ✅ 5. Validate Serving
- **Job**: `src/jobs/validate_serving.py`
- **Mô tả**: Validate dữ liệu serving: kiểm tra schema, null, duplicates, phân phối.
- **Trạng thái**: Hoàn thành

### ✅ Orchestration với Airflow
- **DAG**: `airflow/dags/Customer_Pipeline_dags.py`
- **Mô tả**: Orchestrate các jobs theo thứ tự.
- **Trạng thái**: Đã tạo DAG

---

## 📊 Output & Kết quả

* **Tổng số khách hàng**: Đã xử lý thành công
* **Phân bố chi tiêu**: Theo segment và city_tier
* **Partitioning**: Theo city_tier (Tier1, Tier2, Tier3)
* **Format**: Parquet cho hiệu năng cao

---

## ⚡ Tối ưu hiệu năng

* Partition theo city_tier và shopping_preference
* Giảm shuffle trong Spark transformations
* Sử dụng monotonically_increasing_id cho customer_id
* Lưu trữ Parquet để query nhanh

---

## 🛠️ Cách chạy project

### Yêu cầu
- Python 3.x
- PySpark
- Airflow (tùy chọn)

### Chạy từng job riêng lẻ
```bash
cd src/jobs
python ingest_raw.py
python clean_data.py
python build_dim_fact.py
python build_serving.py
python validate_serving.py
```

### Chạy với Airflow
```bash
airflow dags unpause Customer_Pipeline_dags
airflow dags trigger Customer_Pipeline_dags
```

### Kiểm tra output
```bash
ls -la data/clean/
ls -la data/serving/
```

---

## 🚀 Hướng phát triển tiếp

* Tích hợp Kafka cho real-time pipeline
* Monitoring với Data Quality checks
* Deploy lên cloud (AWS EMR, GCP Dataproc)
* Thêm CI/CD pipeline

---

## 🎯 Những gì học được

* Xây dựng ETL pipeline end-to-end
* Data modeling (Fact & Dimension)
* Tối ưu Spark jobs
* Partitioning strategy
* Workflow chuẩn Data Engineer
* Orchestration với Airflow

---

## 📚 Tài liệu tham khảo

* Fundamentals of Data Engineering
* Data Engineering Design Patterns
* PySpark documentation

---

## 👨‍💻 Tác giả

* Họ và Tên: Lê Tùng Lâm
* MSSV: 20235962
* Đại học Bách khoa Hà Nội (HUST) - K68
