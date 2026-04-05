SELECT DISTINCT
    artist_name AS term_key
FROM {{ ref('stg_tracks') }}
WHERE artist_name IS NOT NULL