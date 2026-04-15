# 🚀 Customer Data Pipeline (PySpark)

## 📖 Tổng quan

Đây là project xây dựng** ****pipeline xử lý dữ liệu khách hàng** sử dụng** ** **PySpark** , mô phỏng quy trình Data Engineering trong thực tế.

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
[Extract]
      ↓
[Clean + Validate]
      ↓
[Transform]
  - Thêm customer_id
  - Feature engineering
      ↓
[Data Modeling]
  - Dim table
  - Fact table
      ↓
[Serving Layer]
  - customer_mart (Parquet)
```

Pipeline tuân theo** ** **Data Engineering Lifecycle** :

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
* Airflow (orchestration - tùy chọn)
* Git (quản lý version)

---

## 📂 Cấu trúc project

```text
customer_data_pipeline/
│
├── data/
│   ├── raw/        # dữ liệu thô
│   ├── clean/      # dữ liệu đã xử lý
│   ├── serving/    # dữ liệu phục vụ phân tích
│
├── src/
│   ├── pipeline.py
│   ├── config.py
│
├── dags/ (optional)
│   └── airflow_dag.py
│
├── README.md
└── requirements.txt
```

---

## 🔄 Các bước trong pipeline

### 1. Extract

* Đọc dữ liệu CSV vào Spark DataFrame

### 2. Data Cleaning

* Xóa dữ liệu trùng
* Xử lý giá trị null
* Chuẩn hóa schema

### 3. Feature Engineering

* Tạo cột** **`total_monthly_spend`
* Sinh** **`customer_id` để join

### 4. Data Modeling

* **Dimension Table** : thông tin khách hàng
* **Fact Table** : hành vi khách hàng

👉 Áp dụng mô hình Data Warehouse (Kimball)

---

## 📊 Output

* Tổng số khách hàng
* Phân bố chi tiêu
* Phân tích theo segment

---

## ⚡ Tối ưu hiệu năng

* Partition theo:
  * city_tier
  * shopping_preference
* Giảm shuffle trong Spark
* Sử dụng transformation hiệu quả

---

## ✅ Data Validation

* Kiểm tra schema
* Kiểm tra null
* Kiểm tra duplicate
* Kiểm tra phân phối dữ liệu

---

## 🛠️ Cách chạy project

```bash
pip install -r requirements.txt
python src/pipeline.py
```

---

## 🚀 Hướng phát triển tiếp

* Tích hợp Kafka (real-time pipeline)
* Dùng Airflow để orchestration
* Thêm Data Quality monitoring
* Deploy lên AWS / GCP

---

## 🎯 Những gì học được

* Xây dựng ETL pipeline end-to-end
* Data modeling (Fact & Dimension)
* Tối ưu Spark job
* Partitioning strategy
* Workflow chuẩn Data Engineer

---

## 📚 Tài liệu tham khảo

* Fundamentals of Data Engineering
* Data Engineering Design Patterns

---

## 👨‍💻 Tác giả

* Họ và Tên: Lê Tùng Lâm
* MSSV: 20235962
* Đại học Bách khoa Hà Nội (HUST) - K68
