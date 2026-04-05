select
    t.track_id as track_key,
    to_date(t.ingested_at) as time_key,
    t.source_term as term_key,
    t.artist_name,
    t.country as country_code,
    t.track_price
from {{ ref('stg_tracks') }} t
where t.track_id is not null
  and t.ingested_at is not null
  and t.source_term is not null
  and t.artist_name is not null