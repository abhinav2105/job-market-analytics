import subprocess
import sys
import os
from datetime import datetime
from dotenv import load_dotenv

from prefect import flow, task, get_run_logger

load_dotenv()

# ── Tasks ────────────────────────────────────────────────

@task(name="Scrape Jobs to Bronze", retries=2, retry_delay_seconds=60)
def scrape_jobs():
    logger = get_run_logger()
    logger.info("🔍 Starting job scraping...")

    project_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

    result = subprocess.run(
        [sys.executable, "ingestion/scrape_to_snowflake.py"],
        capture_output=True,
        text=True,
        cwd=project_dir
    )

    if result.returncode != 0:
        logger.error(f"❌ Scraping failed:\n{result.stderr}")
        raise Exception(f"Scraping failed: {result.stderr}")

    logger.info(f"✅ Scraping complete:\n{result.stdout}")
    return result.stdout

@task(name="Transform Bronze to Silver", retries=1)
def transform_silver():
    logger = get_run_logger()
    logger.info("🔄 Starting Silver transformation...")

    project_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

    result = subprocess.run(
        [sys.executable, "silver/transform_to_silver.py"],
        capture_output=True,
        text=True,
        cwd=project_dir
    )

    if result.returncode != 0:
        logger.error(f"❌ Silver transform failed:\n{result.stderr}")
        raise Exception(f"Silver transform failed: {result.stderr}")

    logger.info(f"✅ Silver transform complete:\n{result.stdout}")
    return result.stdout

@task(name="Extract Skills", retries=1)
def extract_skills():
    logger = get_run_logger()
    logger.info("🧠 Starting skill extraction...")

    project_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

    result = subprocess.run(
        [sys.executable, "silver/extract_skills.py"],
        capture_output=True,
        text=True,
        cwd=project_dir
    )

    if result.returncode != 0:
        logger.error(f"❌ Skill extraction failed:\n{result.stderr}")
        raise Exception(f"Skill extraction failed: {result.stderr}")

    logger.info(f"✅ Skill extraction complete:\n{result.stdout}")
    return result.stdout

@task(name="Run dbt Models", retries=1)
def run_dbt():
    logger = get_run_logger()
    logger.info("🏗️ Running dbt models...")

    dbt_path = os.path.join(os.path.dirname(sys.executable), "dbt")
    project_dir = os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        "job_market"
    )

    result = subprocess.run(
        [dbt_path, "run"],
        capture_output=True,
        text=True,
        cwd=project_dir
    )

    if result.returncode != 0:
        logger.error(f"❌ dbt run failed:\n{result.stderr}")
        raise Exception(f"dbt run failed: {result.stderr}")

    logger.info(f"✅ dbt run complete:\n{result.stdout}")
    return result.stdout

@task(name="Run dbt Tests", retries=1)
def run_dbt_tests():
    logger = get_run_logger()
    logger.info("🧪 Running dbt tests...")

    dbt_path = os.path.join(os.path.dirname(sys.executable), "dbt")
    project_dir = os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        "job_market"
    )

    result = subprocess.run(
        [dbt_path, "test"],
        capture_output=True,
        text=True,
        cwd=project_dir
    )

    if result.returncode != 0:
        logger.error(f"❌ dbt tests failed:\n{result.stderr}")
        raise Exception(f"dbt tests failed: {result.stderr}")

    logger.info(f"✅ dbt tests passed:\n{result.stdout}")
    return result.stdout

# ── Flow ─────────────────────────────────────────────────

@flow(
    name="Job Market Analytics Pipeline",
    description="Full pipeline: scrape → silver → skills → dbt run → dbt test"
)
def job_market_pipeline():
    logger = get_run_logger()
    start_time = datetime.now()
    logger.info(f"🚀 Pipeline started at {start_time}")

    # Step 1 — Scrape
    scrape_jobs()

    # Step 2 — Silver transform
    transform_silver()

    # Step 3 — Skill extraction
    extract_skills()

    # Step 4 — dbt run
    run_dbt()

    # Step 5 — dbt tests
    run_dbt_tests()

    end_time = datetime.now()
    duration = (end_time - start_time).seconds
    logger.info(f"🎉 Pipeline complete in {duration} seconds!")

if __name__ == "__main__":
    job_market_pipeline()
