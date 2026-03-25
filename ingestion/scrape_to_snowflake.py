import snowflake.connector
import requests
from dotenv import load_dotenv
import os
import time

load_dotenv()

MAX_JOBS_PER_SOURCE = 50

# ── Connections ──────────────────────────────────────────
def get_snowflake_conn():
    return snowflake.connector.connect(
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        role=os.getenv("SNOWFLAKE_ROLE")
    )

# ── Source 1: Adzuna API ─────────────────────────────────
def scrape_adzuna(keyword, max_results=MAX_JOBS_PER_SOURCE):
    print(f"🔍 Adzuna: '{keyword}'...")
    app_id = os.getenv("ADZUNA_APP_ID")
    app_key = os.getenv("ADZUNA_APP_KEY")

    url = "https://api.adzuna.com/v1/api/jobs/us/search/1"
    params = {
        "app_id": app_id,
        "app_key": app_key,
        "what": keyword,
        "results_per_page": max_results,
        "max_days_old": 7,
        "content-type": "application/json"
    }

    response = requests.get(url, params=params)
    data = response.json()

    jobs = []
    for job in data.get("results", []):
        jobs.append({
            "id": str(job.get("id", "")),
            "title": job.get("title", ""),
            "company": job.get("company", {}).get("display_name", ""),
            "company_url": "",
            "location": job.get("location", {}).get("display_name", ""),
            "job_url": job.get("redirect_url", ""),
            "posted_at": job.get("created", ""),
            "salary": f"{job.get('salary_min', '')} - {job.get('salary_max', '')}",
            "job_type": job.get("contract_time", ""),
            "experience_level": "",
            "description": job.get("description", ""),
            "applicant_count": "",
            "remote_flag": "",
            "source_platform": "adzuna"
        })

    jobs = jobs[:MAX_JOBS_PER_SOURCE]
    print(f"✅ Got {len(jobs)} jobs from Adzuna for '{keyword}'")
    return jobs

# ── Source 2: The Muse API ───────────────────────────────
def scrape_the_muse(keyword, max_results=MAX_JOBS_PER_SOURCE):
    print(f"🔍 The Muse: '{keyword}'...")

    url = "https://www.themuse.com/api/public/jobs"
    params = {
        "page": 0,
        "descending": "true"
    }

    response = requests.get(url, params=params)
    data = response.json()

    jobs = []
    keyword_lower = keyword.lower()

    for job in data.get("results", []):
        title = job.get("name", "").lower()
        if keyword_lower not in title:
            continue

        locations = job.get("locations", [])
        location = locations[0].get("name", "") if locations else ""

        jobs.append({
            "id": str(job.get("id", "")),
            "title": job.get("name", ""),
            "company": job.get("company", {}).get("name", ""),
            "company_url": job.get("company", {}).get("url", ""),
            "location": location,
            "job_url": job.get("refs", {}).get("landing_page", ""),
            "posted_at": job.get("publication_date", ""),
            "salary": "",
            "job_type": job.get("type", ""),
            "experience_level": job.get("levels", [{}])[0].get("name", "") if job.get("levels") else "",
            "description": job.get("contents", ""),
            "applicant_count": "",
            "remote_flag": "",
            "source_platform": "the_muse"
        })

        if len(jobs) >= max_results:
            break

    jobs = jobs[:MAX_JOBS_PER_SOURCE]
    print(f"✅ Got {len(jobs)} jobs from The Muse for '{keyword}'")
    return jobs

# ── Source 3: RemoteOK API ───────────────────────────────
def scrape_remoteok(keyword, max_results=MAX_JOBS_PER_SOURCE):
    print(f"🔍 RemoteOK: '{keyword}'...")

    url = "https://remoteok.com/api"
    headers = {"User-Agent": "job-market-analytics-project"}

    response = requests.get(url, headers=headers)
    data = response.json()

    jobs = []
    keyword_lower = keyword.lower()

    for job in data:
        if not isinstance(job, dict):
            continue

        title = job.get("position", "").lower()
        tags = " ".join(job.get("tags", [])).lower()

        if keyword_lower not in title and keyword_lower not in tags:
            continue

        jobs.append({
            "id": str(job.get("id", "")),
            "title": job.get("position", ""),
            "company": job.get("company", ""),
            "company_url": job.get("company_url", ""),
            "location": "Remote",
            "job_url": job.get("url", ""),
            "posted_at": job.get("date", ""),
            "salary": job.get("salary", ""),
            "job_type": "remote",
            "experience_level": "",
            "description": job.get("description", ""),
            "applicant_count": "",
            "remote_flag": "True",
            "source_platform": "remoteok"
        })

        if len(jobs) >= max_results:
            break

    jobs = jobs[:MAX_JOBS_PER_SOURCE]
    print(f"✅ Got {len(jobs)} jobs from RemoteOK for '{keyword}'")
    return jobs

# ── Validation ───────────────────────────────────────────
def validate_scrape(jobs, source, keyword, min_expected=1):
    if len(jobs) < min_expected:
        print(f"⚠️ Warning: Only got {len(jobs)} jobs from {source} for '{keyword}'")

# ── Loading ──────────────────────────────────────────────
def load_to_bronze(items, conn):
    cursor = conn.cursor()
    inserted = 0

    for job in items:
        cursor.execute("""
            INSERT INTO JOB_MARKET.BRONZE.RAW_LINKEDIN_JOBS (
                id, title, company, company_url, location,
                job_url, posted_at, salary, job_type,
                experience_level, description, applicant_count,
                remote_flag, source_platform
            ) VALUES (
                %s, %s, %s, %s, %s,
                %s, %s, %s, %s,
                %s, %s, %s,
                %s, %s
            )
        """, (
            job["id"],
            job["title"],
            job["company"],
            job["company_url"],
            job["location"],
            job["job_url"],
            job["posted_at"],
            job["salary"],
            job["job_type"],
            job["experience_level"],
            job["description"],
            job["applicant_count"],
            job["remote_flag"],
            job["source_platform"]
        ))
        inserted += 1

    conn.commit()
    print(f"✅ Inserted {inserted} rows into Bronze layer")

def truncate_bronze(conn):
    cursor = conn.cursor()
    cursor.execute("TRUNCATE TABLE JOB_MARKET.BRONZE.RAW_LINKEDIN_JOBS")
    conn.commit()
    print("🗑️ Bronze table truncated")

# ── Main ─────────────────────────────────────────────────
def main():
    conn = get_snowflake_conn()
    print("✅ Snowflake connected\n")
    
    # truncate bronze before each run
    truncate_bronze(conn)
    
    # rest of the code...
    roles = [
        "data analyst",
        "data engineer",
        "python developer",
        "cloud engineer",
        "business intelligence analyst",
        "software developer"
    ]

    conn = get_snowflake_conn()
    print("✅ Snowflake connected\n")

    total_inserted = 0

    for role in roles:
        print(f"\n── Role: {role} ──────────────────────")
        all_jobs = []

        # Adzuna
        try:
            jobs = scrape_adzuna(role)
            validate_scrape(jobs, "Adzuna", role)
            all_jobs.extend(jobs)
        except Exception as e:
            print(f"❌ Adzuna failed for '{role}': {e}")

        # The Muse
        try:
            jobs = scrape_the_muse(role)
            validate_scrape(jobs, "The Muse", role)
            all_jobs.extend(jobs)
        except Exception as e:
            print(f"❌ The Muse failed for '{role}': {e}")

        # RemoteOK
        try:
            jobs = scrape_remoteok(role)
            validate_scrape(jobs, "RemoteOK", role)
            all_jobs.extend(jobs)
        except Exception as e:
            print(f"❌ RemoteOK failed for '{role}': {e}")

        if all_jobs:
            load_to_bronze(all_jobs, conn)
            total_inserted += len(all_jobs)

        # be polite to APIs
        time.sleep(2)

    conn.close()
    print(f"\n🎉 Bronze layer load complete! Total jobs inserted: {total_inserted}")

if __name__ == "__main__":
    main()
