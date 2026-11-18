# Game Analytics Data Lakehouse & AI Recommender

Đồ án tốt nghiệp xây dựng một Data Lakehouse để xử lý metadata, đánh giá và chỉ số độ phổ biến của trò chơi từ Steam và SteamSpy. Dữ liệu được tổ chức để phân tích qua Dremio và Power BI, đồng thời phục vụ tìm kiếm và gợi ý game trong ứng dụng AI trên Streamlit.

## Tính năng chính

- **Thu thập và ETL.** Collector chuẩn bị CSV và bổ sung tags trong staging. Airflow điều phối nạp dữ liệu incremental vào Bronze và chuyển đổi qua Silver, Gold.
- **Analytics và BI.** Dremio truy vấn dữ liệu Gold. Dashboard Power BI của đồ án dựa trên các view Dremio lịch sử theo thông tin từ Project Owner. SQL V2 có contract riêng dành cho kiểm thử.
- **Ứng dụng Streamlit.**
  - **Search** tìm theo từ khóa trong metadata như tên, tags và thể loại.
  - **Recommend** dùng FAISS để gợi ý game tương tự từ một game gốc.
  - **Chatbot** định tuyến câu hỏi theo intent. Hai nhánh tìm kiếm và gợi ý dùng hybrid ranking. Các nhánh khác hỗ trợ so sánh hai game hoặc phân tích một game hay phân khúc.
- **Recommender evaluation.** Mặc định tắt. Khi bật, ứng dụng đo mức khớp của ranking với nhãn tham chiếu tạo từ metadata (pseudo ground truth).

## Kiến trúc và công nghệ

`Steam/SteamSpy → CSV → Bronze → Silver → Gold`. Bronze upsert từng CSV, Silver chuẩn hóa dữ liệu và lưu lịch sử SCD Type 2 cho các trường theo dõi, Gold tổ chức dimensions, bridges và facts theo trạng thái hiện hành. BI truy vấn Gold. Bộ tạo index của ứng dụng AI sử dụng dữ liệu từ Silver và Gold. [Sơ đồ và chi tiết kiến trúc](docs/architecture.md)

| Mảng | Công nghệ | Vai trò |
| --- | --- | --- |
| Điều phối | Apache Airflow | Lập lịch nạp Bronze và điều phối tác vụ Silver, Gold |
| Xử lý | Spark/PySpark, pandas | Đọc, chuẩn hóa và ghi dữ liệu ETL |
| Lưu trữ và catalog | MinIO, Apache Iceberg, Hive Metastore, PostgreSQL | Lưu CSV và bảng Iceberg, quản lý metadata catalog |
| Phân tích | Dremio, Power BI | Truy vấn Gold và trình bày kết quả trên dashboard |
| Ứng dụng AI | Streamlit, FAISS, Chroma, Ollama | Giao diện, chỉ mục tương đồng và truy xuất ngữ nghĩa |

## Bắt đầu tìm hiểu

Để tái lập Lakehouse, cần môi trường Docker Compose tương thích cho Airflow, MinIO, Spark và Hive Metastore với PostgreSQL. Bộ CSV đầu vào Airflow gồm `games.csv` có cột `tags`, `reviews.csv` và `popularity.csv`. Phần AI sử dụng FAISS index và metadata tương ứng. [Cấu hình Compose tham chiếu](LakeHouse_Project/docker-compose.yml) · [Chuẩn bị CSV trong staging](docs/collector-staging.md)

### Ba DAG theo thứ tự dữ liệu

| DAG | Vai trò | Lịch hoặc trigger |
| --- | --- | --- |
| [`auto_upload_bronze_to_minio`](LakeHouse_Project/airflow/dags/auto_upload_data.py) | Nạp CSV vào MinIO Bronze | Mỗi hai phút (`*/2 * * * *`) |
| [`bronze_to_silver_etl`](LakeHouse_Project/airflow/dags/bronze_to_silver_etl.py) | Chuẩn hóa Bronze thành các bảng Silver | Trigger riêng (`schedule=None`) |
| [`gold_layer_etl_complete`](LakeHouse_Project/airflow/dags/silver_to_gold_etl.py) | Xây dựng và xác minh các bảng Gold | Trigger riêng (`schedule=None`) |

Xem [ETL scripts](LakeHouse_Project/scripts) · [Collector](collectors) · [SQL và Power BI](docs/sql-analytics.md) · [Tính năng AI](LakeHouse_Project/streamlit-ai/AI_FEATURES_DOCUMENT.md) · [Evaluation](LakeHouse_Project/streamlit-ai/evaluation/README_RECOMMENDER_EVALUATION.md)

## Kiểm thử offline và giới hạn

Chạy từ repository root bằng Python với pandas và NumPy. Các suite dùng source, mocks hoặc dữ liệu tổng hợp. Một số ca trong suite core có thể skip tùy dependency native.

```text
python -B -m unittest discover -s tests/collectors -v
python -B -m unittest discover -s tests/sql -v
python -B -m unittest discover -s tests/core -v
python -B -m unittest discover -s tests/evaluation -v
python -B -m unittest discover -s tests/reliability -v
```

- Collector kiểm tra batch trong staging. Bàn giao cả bộ CSV sang Airflow cần quy trình riêng. Các lần ghi có ranh giới theo object hoặc bảng. Chi tiết retry và recovery nằm trong tài liệu kiến trúc và test plan. [Kiến trúc](docs/architecture.md) · [Reliability test plan](docs/reliability-test-plan.md)
- Trạng thái native end-to-end của Lakehouse, các tích hợp AI và live Steam/SteamSpy chưa được xác minh đầy đủ. Thực thi và triển khai SQL V2 trên Dremio, PBIX bindings và refresh Power BI cần xác minh riêng. [Giới hạn kiểm thử BI](docs/sql-analytics.md)
