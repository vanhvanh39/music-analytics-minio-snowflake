with artist_daily_metrics as (
    select
        artist_name as term_key,
        time_key,
        count(distinct track_key) as track_count,
        avg(track_price) as avg_price
    from {{ ref('fact_track_snapshot_hourly') }}
    group by artist_name, time_key
)

select
    term_key,
    time_key,
    track_count,
    track_count - lag(track_count) over (
        partition by term_key
        order by time_key
    ) as track_count_delta,
    avg_price,
    avg_price - lag(avg_price) over (
        partition by term_key
        order by time_key
    ) as avg_price_delta
from artist_daily_metrics