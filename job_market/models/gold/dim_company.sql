WITH companies AS (
    SELECT DISTINCT
        company_normalized AS company_name
    FROM {{ ref('stg_jobs') }}
    WHERE company_normalized != ''
),

company_stats AS (
    SELECT
        company_normalized AS company_name,
        COUNT(*) AS total_jobs_posted,
        AVG(salary_min) AS avg_salary_min,
        AVG(salary_max) AS avg_salary_max,
        COUNT(CASE WHEN remote_flag = TRUE THEN 1 END) AS remote_jobs_count
    FROM {{ ref('stg_jobs') }}
    GROUP BY company_normalized
)

SELECT
    MD5(c.company_name) AS company_id,
    c.company_name,
    cs.total_jobs_posted,
    cs.avg_salary_min,
    cs.avg_salary_max,
    cs.remote_jobs_count
FROM companies c
LEFT JOIN company_stats cs ON c.company_name = cs.company_name