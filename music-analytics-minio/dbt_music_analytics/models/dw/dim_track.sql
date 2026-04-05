SELECT DISTINCT
    track_id,
    track_name,
    artist_id,
    collection_id AS album_id,
    genre
FROM {{ ref('stg_tracks') }}
WHERE track_id IS NOT NULL