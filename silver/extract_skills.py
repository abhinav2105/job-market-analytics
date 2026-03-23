import snowflake.connector
from dotenv import load_dotenv
import os
import hashlib
import re

load_dotenv()

# ── Connection ───────────────────────────────────────────
def get_snowflake_conn():
    return snowflake.connector.connect(
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        role=os.getenv("SNOWFLAKE_ROLE")
    )

# ── Skills Taxonomy ──────────────────────────────────────
SKILLS_TAXONOMY = {
    "languages": [
        "python", "sql", "t-sql", "pl/sql", "mysql",
        "nosql", "r programming", "scala", "java",
        "javascript", "typescript", "bash", "shell",
        "golang", "c++", "c#", "structured query language"
    ],
    "cloud": [
        "aws", "azure", "gcp", "google cloud", "s3", "ec2",
        "lambda", "redshift", "bigquery", "snowflake", "databricks",
        "sagemaker", "glue", "athena", "cloudformation", "terraform"
    ],
    "tools": [
        "dbt", "airflow", "spark", "kafka", "docker", "kubernetes",
        "git", "jenkins", "prefect", "luigi", "dagster", "flink",
        "hadoop", "hive", "pandas", "numpy", "scikit-learn",
        "pytorch", "tensorflow", "mlflow", "great expectations"
    ],
    "databases": [
        "postgresql", "postgres", "mongodb", "redis",
        "cassandra", "dynamodb", "elasticsearch", "oracle",
        "sql server", "sqlite", "bigquery"
    ],
    "visualization": [
        "tableau", "power bi", "powerbi", "looker", "quicksight",
        "streamlit", "plotly", "matplotlib", "seaborn", "grafana",
        "metabase", "superset", "d3.js"
    ],
    "concepts": [
        "machine learning", "deep learning", "nlp", "etl", "elt",
        "data modeling", "data warehouse", "data lake", "lakehouse",
        "rest api", "microservices", "agile", "scrum", "ci/cd",
        "devops", "mlops", "statistics", "data governance"
    ]
}

REQUIRED_KEYWORDS = [
    "required", "must have", "must-have", "essential",
    "you must", "you will need", "mandatory", "need to have"
]

# ── Skill Extraction ─────────────────────────────────────
def extract_skills(job_id, description):
    if not description:
        return []

    description_lower = description.lower()
    found_skills = []

    for category, skills in SKILLS_TAXONOMY.items():
        for skill in skills:
            # use word boundary matching
            pattern = r'\b' + re.escape(skill.lower()) + r'\b'
            if re.search(pattern, description_lower):
                idx = description_lower.find(skill.lower())
                surrounding = description_lower[max(0, idx-150):idx+150]

                is_required = any(
                    kw in surrounding for kw in REQUIRED_KEYWORDS
                )

                skill_id = hashlib.md5(
                    f"{job_id}{skill}".encode()
                ).hexdigest()

                found_skills.append((
                    skill_id,
                    str(job_id),
                    skill,
                    category,
                    is_required
                ))

    return found_skills

# ── Main ─────────────────────────────────────────────────
def main():
    conn = get_snowflake_conn()
    cursor = conn.cursor()
    print("✅ Snowflake connected\n")

    cursor.execute("""
        SELECT job_id, description
        FROM JOB_MARKET.SILVER.JOBS
        WHERE description IS NOT NULL
        AND description != ''
    """)
    jobs = cursor.fetchall()
    print(f"📥 Processing {len(jobs)} job descriptions...\n")

    all_skills = []
    jobs_with_skills = 0

    for job_id, description in jobs:
        skills = extract_skills(job_id, description)
        if skills:
            all_skills.extend(skills)
            jobs_with_skills += 1

    print(f"✅ Extracted {len(all_skills)} skills — now loading to Snowflake...")

    if all_skills:
        cursor.executemany("""
            INSERT INTO JOB_MARKET.SILVER.JOB_SKILLS (
                skill_id, job_id, skill_name,
                skill_category, is_required
            ) VALUES (%s, %s, %s, %s, %s)
        """, all_skills)

    conn.commit()
    conn.close()

    print(f"✅ Jobs processed:         {len(jobs)}")
    print(f"✅ Jobs with skills:       {jobs_with_skills}")
    print(f"✅ Total skills extracted: {len(all_skills)}")
    print(f"\n🎉 Skill extraction complete!")

if __name__ == "__main__":
    main()