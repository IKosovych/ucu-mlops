from datetime import datetime
import os
from pathlib import Path
import subprocess

from airflow.decorators import dag, task


@task
def run_scraper_feed():
    script_path = Path("/usr/local/airflow/include/scripts/scraper_feed.py")
    env = os.environ.copy()
    timestamp = datetime.utcnow().strftime("%Y-%m-%dT%H-%M-%S")
    filename = f"msn/feed_stories_{timestamp}.csv"

    env["SCRAPER_OUTPUT_FILENAME"] = filename

    result = subprocess.run(["python", str(script_path)], check=True, env=env)
    return filename


@task
def process_scraped_data(filename: str) -> str:
    script_path = Path("/usr/local/airflow/include/scripts/process_feed.py")
    env = os.environ.copy()
    env["SCRAPER_OUTPUT_FILENAME"] = filename

    result = subprocess.run(["python", str(script_path)], check=True, env=env)

    return "Data processing finished successfully"


@dag(
    dag_id="msn_news_scraper_dag",
    start_date=datetime(2025, 7, 1),
    schedule="@daily",
    catchup=False,
    default_args={"owner": "airflow", "retries": 1},
    tags=["scraper", "news", "msn"]
)
def scraper_feed_dag():
    filename = run_scraper_feed()
    cleaned_filename = process_scraped_data(filename)


scraper_feed_dag()
