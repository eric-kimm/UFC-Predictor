import subprocess
from prefect import flow, task, get_run_logger

@task(retries=2, retry_delay_seconds=60)
def run_scrapy_spider(spider_name: str):
  result = subprocess.run(
    ["scrapy", "crawl", spider_name],
    capture_output=True,
    text=True,
    check=True
  )
  return result.stdout

@flow(log_prints=True)
def scraping_pipeline():
  print("Starting Webscraper")
  run_scrapy_spider('ufc')


if __name__ == "main":
  scraping_pipeline.serve('weekly-scrape', cron="0 0 * * * SAT")