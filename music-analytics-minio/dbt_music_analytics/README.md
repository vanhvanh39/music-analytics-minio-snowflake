# dbt project for Music Analytics

Project này chuyển phần SQL thủ công sang dbt models.

## Cấu trúc
- `models/staging/stg_tracks.sql` -> làm sạch RAW sang STG
- `models/warehouse/` -> các dimension và fact trong DW
- `models/marts/` -> các bảng tổng hợp cho dashboard

## Cài dbt
```bash
python -m pip install dbt-core dbt-snowflake
```

## Tạo profiles
Copy `profiles.yml.example` thành:
- Windows: `%USERPROFILE%\\.dbt\\profiles.yml`
- macOS/Linux: `~/.dbt/profiles.yml`

## Chạy dbt
Trong thư mục project này:
```bash
dbt debug
dbt run --vars '{search_term: "taylor swift"}'
dbt test
```

## Các model sẽ tạo ra
- `MUSIC_DB.STG.STG_TRACKS`
- `MUSIC_DB.DW.DIM_TRACK`
- `MUSIC_DB.DW.DIM_ARTIST`
- `MUSIC_DB.DW.DIM_ALBUM`
- `MUSIC_DB.DW.DIM_TIME`
- `MUSIC_DB.DW.DIM_TERM`
- `MUSIC_DB.DW.FACT_TRACK_SNAPSHOT_HOURLY`
- `MUSIC_DB.MART.AGG_TERM_DAILY_METRICS`
- `MUSIC_DB.MART.AGG_TERM_DAILY_DELTA`
