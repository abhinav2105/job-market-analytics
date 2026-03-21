WITH jobs AS (
    SELECT * FROM {{ ref('stg_jobs') }}
),

companies AS (
    SELECT * FROM {{ ref('dim_company') }}
),

locations AS (
    SELECT * FROM {{ ref('dim_location') }}
),

dates AS (
    SELECT * FROM {{ ref('dim_date') }}
)

SELECT DISTINCT
    j.job_id,
    c.company_id,
    l.location_id,
    d.date_id,
    j.title_raw,
    j.title_normalized,
    j.seniority_level,
    j.salary_min,
    j.salary_max,
    j.salary_raw,
    j.job_type,
    j.experience_level,
    j.description,
    j.source_platform,
    j.job_url,
    j.repost_count,
    j.date_first_posted,
    j.date_last_seen,
    DATEDIFF('day', j.date_first_posted, j.date_last_seen) AS days_open
FROM jobs j
LEFT JOIN companies c ON j.company_normalized = c.company_name
LEFT JOIN locations l
    ON COALESCE(j.city, j.location) = l.city
    AND COALESCE(j.state, '') = COALESCE(l.state, '')
LEFT JOIN dates d ON j.date_first_posted = d.date_day
QUALIFY ROW_NUMBER() OVER (PARTITION BY j.job_id ORDER BY j.date_first_posted) = 1