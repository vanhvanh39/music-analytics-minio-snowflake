SELECT DISTINCT
    TO_DATE(release_date) AS time_key,
    EXTRACT(YEAR FROM TO_DATE(release_date)) AS year,
    EXTRACT(MONTH FROM TO_DATE(release_date)) AS month,
    EXTRACT(DAY FROM TO_DATE(release_date)) AS day
FROM {{ ref('stg_tracks') }}
WHERE release_date IS NOT NULL