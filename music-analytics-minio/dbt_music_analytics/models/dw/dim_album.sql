SELECT DISTINCT
    collection_id AS album_id,
    collection_name AS album_name
FROM {{ ref('stg_tracks') }}
WHERE collection_id IS NOT NULL