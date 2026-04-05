# Music Analytics Pipeline (MinIO + Snowflake)

Project này thay AWS S3 bằng MinIO để bạn chạy local dễ hơn.

## Kiến trúc

```text
iTunes Search API
    -> Python ingest
    -> MinIO (raw)
    -> local curated CSV
    -> Snowflake RAW table
```

## 1) Chuẩn bị

- Python 3.10 hoặc 3.11
- Docker Desktop
- Tài khoản Snowflake

## 2) Cài thư viện

```bash
pip install -r requirements.txt
```

## 3) Tạo file `.env`

Copy file mẫu:

```bash
cp .env.example .env
```

Sau đó sửa các giá trị Snowflake trong `.env`.

## 4) Chạy MinIO

```bash
docker compose up -d
```

- API endpoint: http://localhost:9000
- Console UI: http://localhost:9001
- user: `minioadmin`
- password: `minioadmin123`

## 5) Khởi tạo trong Snowflake

Chạy file `sql/init_snowflake.sql` trong Snowflake Worksheet.

## 6) Chạy toàn bộ pipeline

```bash
python run_pipeline.py
```

## 7) Kiểm tra Snowflake

```sql
SELECT * FROM ITUNES_TRACKS_RAW;
```

## Chạy từng bước

```bash
python scripts/create_minio_buckets.py
python scripts/ingest_itunes_to_minio.py
python scripts/minio_to_local_curated.py
python scripts/load_curated_to_snowflake.py
```

## Lỗi thường gặp

### Không kết nối được MinIO
- Kiểm tra Docker đã chạy chưa
- Kiểm tra `MINIO_ENDPOINT=localhost:9000`

### Snowflake báo lỗi quyền
- Dùng role có quyền tạo table/stage, thường là `ACCOUNTADMIN`
- Chạy `sql/init_snowflake.sql` trước

### Không có file curated
- Hãy chạy `ingest_itunes_to_minio.py` trước rồi mới chạy `minio_to_local_curated.py`
