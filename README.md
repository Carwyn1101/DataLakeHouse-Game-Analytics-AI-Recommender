# Game Analytics Data Lakehouse & AI Recommender

Đồ án tốt nghiệp này xây dựng một Data Lakehouse để tập hợp thông tin trò chơi, đánh giá và số liệu về độ phổ biến từ Steam và SteamSpy. Dữ liệu phục vụ hai phần của đồ án gồm báo cáo phân tích và ứng dụng tìm kiếm, gợi ý game.

## Tính năng chính

- **Thu thập và ETL.** Bộ thu thập chuẩn bị tệp CSV và có thể bổ sung cột `tags` cho dữ liệu trò chơi. Airflow nạp các tệp đã bàn giao vào Bronze và điều phối các bước xử lý Silver, Gold.
- **Phân tích và trực quan hóa.** Dremio dùng để truy vấn dữ liệu tầng Gold. Power BI trực quan hóa các chỉ số về trò chơi trong báo cáo.
- **Ứng dụng Streamlit.**
  - **Search** tìm game theo từ khóa trong tên, thẻ và thể loại.
  - **Recommend** dùng FAISS để gợi ý trò chơi tương tự dựa trên tên một game gốc.
  - **Chatbot** chọn cách xử lý theo nội dung câu hỏi. Hai nhánh tìm kiếm và gợi ý dùng xếp hạng kết hợp (hybrid ranking). Người dùng cũng có thể so sánh hai game hoặc phân tích một game hay một phân khúc.
- **Đánh giá gợi ý.** Tính năng này mặc định tắt. Khi bật, hệ thống đối chiếu thứ hạng gợi ý với các nhãn tham chiếu được tạo từ thông tin trò chơi.

## Kiến trúc và công nghệ

`Steam/SteamSpy → CSV → Bronze → Silver → Gold`. Bronze lưu và cập nhật từng tệp CSV trên MinIO. Silver chuẩn hóa dữ liệu và lưu lịch sử thay đổi của các trường được theo dõi bằng SCD Type 2. Gold xây dựng các bảng chiều, bảng liên kết và bảng số liệu để phân tích trạng thái hiện tại. Ứng dụng AI tạo chỉ mục từ dữ liệu Silver và Gold. [Sơ đồ và chi tiết kiến trúc](docs/architecture.md)

| Mảng | Công nghệ | Vai trò |
| --- | --- | --- |
| Điều phối | Apache Airflow | Lập lịch nạp Bronze và điều phối tác vụ Silver, Gold |
| Xử lý | Spark/PySpark, pandas | Đọc, chuẩn hóa và ghi dữ liệu ETL |
| Lưu trữ | MinIO, Apache Iceberg | Lưu tệp CSV và dữ liệu của các bảng Iceberg |
| Danh mục bảng | Hive Metastore, PostgreSQL | Quản lý thông tin về các bảng Iceberg |
| Phân tích | Dremio, Power BI | Truy vấn dữ liệu Gold và trực quan hóa các chỉ số |
| Ứng dụng AI | Streamlit, FAISS, Chroma, Ollama | Giao diện, gợi ý game và tìm kiếm theo ngữ nghĩa |

## Bắt đầu tìm hiểu

Dữ liệu đầu vào của Airflow gồm `games.csv` có cột `tags`, `reviews.csv` và `popularity.csv`. Ứng dụng Streamlit dùng chỉ mục FAISS và thông tin trò chơi đi kèm. Các dịch vụ của đồ án được mô tả trong cấu hình Docker Compose. [Cấu hình Docker Compose](LakeHouse_Project/docker-compose.yml) · [Chuẩn bị CSV](docs/collector-staging.md)

### Ba DAG theo thứ tự xử lý dữ liệu

| DAG | Vai trò | Lịch chạy |
| --- | --- | --- |
| [`auto_upload_bronze_to_minio`](LakeHouse_Project/airflow/dags/auto_upload_data.py) | Nạp và cập nhật CSV trên MinIO Bronze | Mỗi hai phút (`*/2 * * * *`) |
| [`bronze_to_silver_etl`](LakeHouse_Project/airflow/dags/bronze_to_silver_etl.py) | Chuẩn hóa dữ liệu Bronze thành các bảng Silver | Kích hoạt riêng (`schedule=None`) |
| [`gold_layer_etl_complete`](LakeHouse_Project/airflow/dags/silver_to_gold_etl.py) | Xây dựng và kiểm tra các bảng Gold | Kích hoạt riêng (`schedule=None`) |

Xem [Mã ETL](LakeHouse_Project/scripts) · [Bộ thu thập dữ liệu](collectors) · [SQL và Power BI](docs/sql-analytics.md) · [Tính năng AI](LakeHouse_Project/streamlit-ai/AI_FEATURES_DOCUMENT.md) · [Đánh giá gợi ý](LakeHouse_Project/streamlit-ai/evaluation/README_RECOMMENDER_EVALUATION.md)

## Kiểm thử và giới hạn

Có thể chạy các lệnh Python dưới đây từ thư mục gốc của dự án để kiểm tra bộ thu thập dữ liệu, truy vấn SQL, các chức năng AI, cách đánh giá gợi ý và cách luồng xử lý dữ liệu phản ứng khi có lỗi. Các bài kiểm thử chủ yếu dùng dữ liệu mẫu và thành phần mô phỏng.

```text
python -B -m unittest discover -s tests/collectors -v
python -B -m unittest discover -s tests/sql -v
python -B -m unittest discover -s tests/core -v
python -B -m unittest discover -s tests/evaluation -v
python -B -m unittest discover -s tests/reliability -v
```

- Bộ thu thập kiểm tra dữ liệu trước khi bàn giao. Việc chuyển cả bộ CSV sang Airflow là một bước riêng. Các trường hợp lỗi và cách xử lý được mô tả trong tài liệu kỹ thuật. [Kiến trúc](docs/architecture.md) · [Kế hoạch kiểm thử](docs/reliability-test-plan.md)
- Việc chạy trọn luồng Lakehouse và kết nối trực tiếp với Steam, SteamSpy và các dịch vụ AI còn cần kiểm thử thêm.
- SQL V2 là bộ truy vấn phân tích dành cho kiểm thử riêng. Việc chạy và triển khai SQL V2 trên Dremio, cũng như kết nối dữ liệu và làm mới báo cáo Power BI, chưa được xác minh đầy đủ. [Tài liệu SQL và Power BI](docs/sql-analytics.md)
