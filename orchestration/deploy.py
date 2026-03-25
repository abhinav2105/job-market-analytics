from prefect import flow
from prefect.client.schemas.schedules import CronSchedule
from orchestration.pipeline import job_market_pipeline

if __name__ == "__main__":
    job_market_pipeline.serve(
        name="job-market-weekly",
        schedule=CronSchedule(cron="0 6 * * 1"),  # every Monday at 6am
        tags=["job-market", "weekly"]
    )