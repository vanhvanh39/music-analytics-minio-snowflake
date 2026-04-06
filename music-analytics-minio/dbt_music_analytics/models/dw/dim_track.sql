WITH ranked_tracks AS (
    SELECT
        track_id AS track_key,
        track_id,
        track_name,
        artist_id,
        collection_id AS album_id,
        genre,
        ROW_NUMBER() OVER (
            PARTITION BY track_id
            ORDER BY track_name, artist_id, collection_id
        ) AS rn
    FROM {{ ref('stg_tracks') }}
    WHERE track_id IS NOT NULL
)

SELECT
    track_key,
    track_id,
    track_name,
    artist_id,
    album_id,
    genre
FROM ranked_tracks
WHERE rn = 1