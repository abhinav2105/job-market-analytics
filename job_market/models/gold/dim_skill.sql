WITH skills AS (
    SELECT DISTINCT
        skill_name,
        skill_category
    FROM JOB_MARKET.SILVER.JOB_SKILLS
),

skill_stats AS (
    SELECT
        skill_name,
        COUNT(*) as total_jobs_requiring,
        COUNT(CASE WHEN is_required = TRUE THEN 1 END) as jobs_requiring,
        COUNT(CASE WHEN is_required = FALSE THEN 1 END) as jobs_preferring
    FROM JOB_MARKET.SILVER.JOB_SKILLS
    GROUP BY skill_name
)

SELECT
    MD5(s.skill_name) AS skill_id,
    s.skill_name,
    s.skill_category,
    ss.total_jobs_requiring,
    ss.jobs_requiring,
    ss.jobs_preferring
FROM skills s
LEFT JOIN skill_stats ss ON s.skill_name = ss.skill_name
ORDER BY ss.total_jobs_requiring DESC