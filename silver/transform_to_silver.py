import snowflake.connector
from dotenv import load_dotenv
import os
import re
import hashlib
from datetime import datetime, date

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

# ── Title Normalization ──────────────────────────────────
SENIORITY_MAP = {
    "sr.": "senior", "sr ": "senior", "senior": "senior",
    "jr.": "junior", "jr ": "junior", "junior": "junior",
    "entry level": "junior", "entry-level": "junior",
    "lead": "lead", "staff": "staff",
    "principal": "principal", "head of": "head",
    "associate": "associate", "mid": "mid",
    "i ": "junior", "ii ": "mid", "iii ": "senior"
}

ROLE_MAP = {
    "data analyst": "Data Analyst",
    "data engineer": "Data Engineer",
    "python developer": "Python Developer",
    "software developer": "Software Developer",
    "software engineer": "Software Engineer",
    "cloud engineer": "Cloud Engineer",
    "business intelligence analyst": "BI Analyst",
    "bi analyst": "BI Analyst",
    "analytics engineer": "Analytics Engineer",
    "machine learning engineer": "ML Engineer",
    "ml engineer": "ML Engineer",
}

def extract_seniority(title):
    title_lower = title.lower()
    for key, value in SENIORITY_MAP.items():
        if key in title_lower:
            return value.capitalize()
    return "Mid"  # default if no seniority found

def normalize_title(title):
    if not title:
        return "", "Unknown"
    
    title_lower = title.lower().strip()
    
    # extract seniority
    seniority = extract_seniority(title_lower)
    
    # match to known role
    for key, normalized in ROLE_MAP.items():
        if key in title_lower:
            return normalized, seniority
    
    # fallback — title case the original
    return title.strip().title(), seniority

# ── Company Normalization ────────────────────────────────
COMPANY_SUFFIXES = [
    ", llc", ", inc", ", ltd", ", co.", ", corp",
    " llc", " inc.", " inc", " ltd", " limited",
    " corporation", " corp.", " corp"
]

def normalize_company(company):
    if not company:
        return ""
    company = company.strip()
    # remove trailing commas and punctuation
    company = company.strip(",").strip(".").strip()
    company_lower = company.lower()
    for suffix in COMPANY_SUFFIXES:
        if company_lower.endswith(suffix):
            company = company[:len(company) - len(suffix)]
    return company.strip().title()

# ── Location Parsing ─────────────────────────────────────
def parse_location(location):
    if not location:
        return "", "", False
    
    location = location.strip()
    remote_flag = any(word in location.lower() 
                     for word in ["remote", "anywhere", "worldwide"])
    
    if remote_flag:
        return "Remote", "", True
    
    # try to split "City, State" format
    parts = location.split(",")
    if len(parts) >= 2:
        city = parts[0].strip()
        state = parts[1].strip()
        return city, state, False
    
    return location, "", False

# ── Salary Parsing ───────────────────────────────────────

def parse_salary(salary_str):
    if not salary_str or salary_str.strip() in ["", " - ", "None - None"]:
        return None, None
    
    # remove currency symbols and text
    cleaned = re.sub(r'[£$€,]', '', str(salary_str))
    cleaned = cleaned.lower().replace('per year', '').replace(
        'per hour', '').replace('annually', '')
    
    def convert_k(val):
        val = val.strip()
        if 'k' in val:
            return float(val.replace('k', '')) * 1000
        return float(val)
    
    numbers = re.findall(r'\d+\.?\d*k?', cleaned)
    
    if len(numbers) >= 2:
        try:
            val1 = convert_k(numbers[0])
            val2 = convert_k(numbers[1])
            if val1 == val2:
                # single value — create ±15% range
                return round(val1 * 0.85, 2), round(val1 * 1.15, 2)
            return min(val1, val2), max(val1, val2)
        except:
            return None, None
    elif len(numbers) == 1:
        try:
            val = convert_k(numbers[0])
            # create ±15% range
            return round(val * 0.85, 2), round(val * 1.15, 2)
        except:
            return None, None
    
    return None, None

# ── Deduplication ────────────────────────────────────────
def make_hash(company, title, location):
    key = f"{company.lower().strip()}{title.lower().strip()}{location.lower().strip()}"
    return hashlib.md5(key.encode()).hexdigest()

def parse_date(date_str):
    if not date_str:
        return date.today()
    try:
        # handle ISO format
        if "T" in str(date_str):
            return datetime.fromisoformat(
                str(date_str).replace("Z", "+00:00")
            ).date()
        return datetime.strptime(str(date_str)[:10], "%Y-%m-%d").date()
    except:
        return date.today()

# ── Main Transform ───────────────────────────────────────
def transform_and_load():
    conn = get_snowflake_conn()
    cursor = conn.cursor()
    print("✅ Snowflake connected\n")

    # fetch all bronze records
    cursor.execute("""
        SELECT id, title, company, location, salary,
               job_type, experience_level, description,
               job_url, posted_at, source_platform
        FROM JOB_MARKET.BRONZE.RAW_LINKEDIN_JOBS
    """)
    bronze_records = cursor.fetchall()
    print(f"📥 Fetched {len(bronze_records)} records from Bronze\n")

    inserted = 0
    updated = 0
    skipped = 0

    for record in bronze_records:
        (raw_id, title, company, location, salary,
         job_type, experience_level, description,
         job_url, posted_at, source_platform) = record

        # ── Clean fields ──
        title_normalized, seniority = normalize_title(title or "")
        company_normalized = normalize_company(company or "")
        city, state, remote_flag = parse_location(location or "")
        salary_min, salary_max = parse_salary(salary or "")
        posted_date = parse_date(posted_at)

        # ── Generate hash for dedup ──
        source_hash = make_hash(
            company_normalized, title_normalized, city or location or ""
        )

        # ── Stage 1: Check exact hash exists ──
        cursor.execute("""
            SELECT job_id, repost_count, date_first_posted
            FROM JOB_MARKET.SILVER.JOBS
            WHERE source_hash = %s
        """, (source_hash,))
        existing = cursor.fetchone()

        if existing:
            existing_id, repost_count, first_posted = existing

            # ── Stage 2: Check if within 45 days ──
            days_diff = (date.today() - first_posted).days
            if days_diff <= 45:
                # update repost info
                cursor.execute("""
                    UPDATE JOB_MARKET.SILVER.JOBS
                    SET date_last_seen = %s,
                        repost_count = repost_count + 1
                    WHERE job_id = %s
                """, (date.today(), existing_id))
                updated += 1
                continue
            else:
                skipped += 1
                continue

        # ── Insert new record ──
        cursor.execute("""
            INSERT INTO JOB_MARKET.SILVER.JOBS (
                job_id, source_hash, title_raw, title_normalized,
                seniority_level, company_raw, company_normalized,
                location, city, state, remote_flag,
                salary_raw, salary_min, salary_max,
                job_type, experience_level, description,
                source_platform, job_url,
                date_first_posted, date_last_seen, repost_count
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
        """, (
            str(raw_id), source_hash, title, title_normalized,
            seniority, company, company_normalized,
            location, city, state, remote_flag,
            salary, salary_min, salary_max,
            job_type, experience_level, description,
            source_platform, job_url,
            posted_date, posted_date, 1
        ))
        inserted += 1

    conn.commit()
    conn.close()

    print(f"✅ Inserted:       {inserted}")
    print(f"🔄 Updated (repost): {updated}")
    print(f"⏭️  Skipped:        {skipped}")
    print(f"\n🎉 Silver layer load complete!")

if __name__ == "__main__":
    transform_and_load()
