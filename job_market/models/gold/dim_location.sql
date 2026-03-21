WITH locations AS (
    SELECT DISTINCT
        COALESCE(city, location) AS city,
        state,
        remote_flag
    FROM {{ ref('stg_jobs') }}
    WHERE location IS NOT NULL
)

SELECT
    MD5(COALESCE(city, '') || COALESCE(state, '') || COALESCE(remote_flag::VARCHAR, '')) AS location_id,
    city,
    state,
    remote_flag,
    CASE
        WHEN remote_flag = TRUE THEN 'Remote'
        WHEN state IS NOT NULL AND state != '' THEN city || ', ' || state
        ELSE city
    END AS location_display
FROM locations