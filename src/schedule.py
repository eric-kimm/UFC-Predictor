import subprocess
from pathlib import Path
import sys
from prefect import flow, task

BASE_DIR = Path(__file__).resolve().parent

@task(retries=2, retry_delay_seconds=60)
def run_scrapy_spider(spider_name: str):
  result = subprocess.run(
    ["scrapy", "crawl", spider_name],
    cwd=str(BASE_DIR / "scrapy"),
    capture_output=True,
    text=True,
    check=True
  )
  return result.stdout

@task(retries=2, retry_delay_seconds=60)
def run_feature_engineering():
  result = subprocess.run(
    [sys.executable, str(BASE_DIR / "feature_engineering.py")],
    cwd=str(BASE_DIR),
    capture_output=True,
    text=True,
    check=True
  )
  return result.stdout


@flow(log_prints=True)
def scraping_pipeline():
  print("Starting Webscraper")
  run_scrapy_spider('ufc')
  run_feature_engineering()

scraping_pipeline.serve('weekly-scrape', cron="0 0 * * * 6")
