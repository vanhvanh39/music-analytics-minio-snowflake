select
    cast(track_id as string) as track_id,
    cast(artist_id as string) as artist_id,
    cast(collection_id as string) as collection_id,
    trim(artist_name) as artist_name,
    trim(collection_name) as collection_name,
    trim(track_name) as track_name,
    trim(primary_genre_name) as genre,
    try_to_double(track_price) as track_price,
    currency,
    try_to_timestamp(release_date) as release_date,
    country,
    ingested_at,
    source_term
from {{ source('raw', 'itunes_tracks_raw') }}
where track_id is not null