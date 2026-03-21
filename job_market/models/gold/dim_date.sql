WITH dates AS (
    SELECT DISTINCT date_first_posted AS date_day
    FROM {{ ref('stg_jobs') }}
    WHERE date_first_posted IS NOT NULL
)

SELECT
    MD5(date_day::VARCHAR) AS date_id,
    date_day,
    DATE_PART('year', date_day) AS year,
    DATE_PART('month', date_day) AS month,
    DATE_PART('day', date_day) AS day,
    DATE_PART('dayofweek', date_day) AS day_of_week,
    DATE_PART('week', date_day) AS week_of_year,
    MONTHNAME(date_day) AS month_name,
    DAYNAME(date_day) AS day_name
FROM dates