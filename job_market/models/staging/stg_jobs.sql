WITH source AS (
    SELECT
        job_id,
        source_hash,
        title_raw,
        title_normalized,
        seniority_level,
        company_raw,
        company_normalized,
        location,
        city,
        state,
        remote_flag,
        salary_raw,
        salary_min,
        salary_max,
        job_type,
        experience_level,
        description,
        source_platform,
        job_url,
        date_first_posted,
        date_last_seen,
        repost_count
    FROM JOB_MARKET.SILVER.JOBS
    WHERE title_normalized IS NOT NULL
        AND company_normalized IS NOT NULL
)

SELECT * FROM source