SELECT DISTINCT
    artist_id AS artist_key,
    artist_id,
    artist_name
FROM {{ ref('stg_tracks') }}
WHERE artist_id IS NOT NULL