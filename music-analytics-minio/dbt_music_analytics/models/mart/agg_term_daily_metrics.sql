select
    term_key,
    time_key,
    count(distinct track_key) as track_count,
    avg(track_price) as avg_price
from {{ ref('fact_track_snapshot_hourly') }}
group by term_key, time_key