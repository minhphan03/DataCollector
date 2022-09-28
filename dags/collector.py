import os
import sys
import logging
from datetime import datetime

from scratches.scraper import scrape
from scrapy.crawler import CrawlerProcess
from airflow.decorators import dag, task
from plugins.operators.influxdb_connector import InfluxDBWriteDictOperator

from src.weather_spider import Scraper

logging.getLogger().setLevel(logging.INFO)

# pick data every hour
@dag(schedule_interval= '0 * * * *', start_date=datetime(2022, 8, 31), catchup=False, render_template_as_native_obj=True)
def scrape_data_flow():
    # two different ways to write mini-tasks

    # mini-task: scrap data
    @task(task_id='scraping_word', retires=2)
    def push_data(**context)->None:
        task_instance = context['ti']

        # run a CrawlerProcess instance on background
        process = CrawlerProcess()
        process.crawl(Scraper, ti=task_instance)
        process.start()
        logging.info('spider crawling weather data finished')
    
    # mini-task: store data to InfluxDB
    weather_data_storing = InfluxDBWriteDictOperator(
        task_id='weather_data_storing',
        bucket_name='weather-data',
        # escape special closing single quotes according to Airflow syntax
        data='{{ ti.xcom_pull(key=\'weather_data\', task_ids=\'scraping_word\') }}',
        influxdb_conn_id='influxdb-connector'
    )

    push_data() >> weather_data_storing

# activate dag
dag = scrape_data_flow()