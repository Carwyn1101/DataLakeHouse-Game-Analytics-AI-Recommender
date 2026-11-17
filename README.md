# Game Analytics Data Lakehouse

Dự án luận văn xây dựng một Lakehouse cho dữ liệu trò chơi Steam. Pipeline chuẩn hóa metadata, review và chỉ số SteamSpy thành dữ liệu phân tích. Dremio phục vụ truy vấn BI, còn Streamlit hỗ trợ tìm kiếm trò chơi, gợi ý từ một game gốc và phân tích bằng Chatbot.

## Luồng dữ liệu

1. Dữ liệu luận văn đi từ Steam và SteamSpy qua CSV. Collector được phục hồi có thể tạo CSV và bổ sung tags trong staging để kiểm tra trước khi bàn giao.
2. Airflow quét thư mục đầu vào mỗi hai phút và upsert từng CSV trên MinIO Bronze theo `steam_appid` chuẩn hóa và timestamp. Silver và Gold đặt `schedule=None` và cần trigger riêng.
3. Silver chuẩn hóa dữ liệu và dùng Spark với Iceberg `MERGE` theo từng bảng để lưu lịch sử SCD Type 2 cho các trường theo dõi. Gold tạo dimensions, bridges và facts theo trạng thái hiện hành. Iceberg lưu các bảng trên MinIO và Hive Metastore quản lý catalog.
4. Power BI của luận văn dùng các view Dremio lịch sử theo thông tin từ Project Owner. SQL V2 định nghĩa contract riêng để kiểm thử, với deployment trên Dremio chưa được xác minh.
5. Streamlit cung cấp lexical Search, FAISS Recommend và Chatbot với hybrid ranking dựa trên tín hiệu có cấu trúc, semantic retrieval qua Chroma/Ollama và review quality. Evaluation tùy chọn đo ranking theo metadata pseudo ground truth.

Sơ đồ, điểm kết nối và trạng thái xác minh được mô tả trong [kiến trúc](docs/architecture.md).

## Cấu trúc dự án

| Thành phần | Vị trí |
| --- | --- |
| Compose, Airflow DAGs và Spark ETL | [Compose](LakeHouse_Project/docker-compose.yml), [DAGs](LakeHouse_Project/airflow/dags), [ETL scripts](LakeHouse_Project/scripts) |
| Collector Steam/SteamSpy và tái lập tags trong staging | [Collector source](collectors), [staging guide](docs/collector-staging.md) |
| Dremio SQL, contract Power BI và phạm vi kiểm thử | [SQL analytics guide](docs/sql-analytics.md) |
| Streamlit, FAISS, Chatbot và recommender evaluation | [AI guide](LakeHouse_Project/streamlit-ai/AI_FEATURES_DOCUMENT.md), [evaluation guide](LakeHouse_Project/streamlit-ai/evaluation/README_RECOMMENDER_EVALUATION.md) |
| Audit, failure scenarios và recovery tests | [Reliability audit](docs/reliability-audit.md), [test plan](docs/reliability-test-plan.md) |
| Đối chiếu tài liệu và bối cảnh luận văn | [Consistency matrix](docs/documentation-audit.md), [ghi chú lịch sử](docs/reference/thesis-context.md) |

## Kiểm thử offline

Từ repository root, dùng Python với pandas và NumPy có sẵn. Lần kiểm thử Phase 4 dùng Python 3.12. Các bài kiểm thử dùng source, mocks hoặc synthetic fixtures. Tùy môi trường, suite core có thể skip bài kiểm tra dependency native.

```text
python -B -m unittest discover -s tests/collectors -v
python -B -m unittest discover -s tests/sql -v
python -B -m unittest discover -s tests/core -v
python -B -m unittest discover -s tests/evaluation -v
python -B -m unittest discover -s tests/reliability -v
```

## Giới hạn đã biết

- Bronze chỉ xử lý các file có mặt và cập nhật từng object riêng. Silver SCD2 tạo phiên bản theo tập trường `SCD2_CHANGE_COLS` của từng bảng. Năm Gold bridges và `fact_game_snapshot` vẫn ghi `DELETE` rồi `APPEND` riêng, nên lỗi giữa hai thao tác có thể để lại dữ liệu thiếu.
- Collector staging kiểm tra từng batch trước khi bàn giao. Việc bàn giao nhiều file sang Airflow vẫn cần một quy trình riêng.
- Trạng thái native end-to-end của pipeline Lakehouse chưa được xác minh. Các kiểm thử offline cũng chưa xác nhận live Steam/SteamSpy, tích hợp native Streamlit/FAISS/Chroma/Ollama, thực thi SQL trên Dremio hoặc refresh dashboard Power BI.
