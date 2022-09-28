import os
import sys
import logging
from datetime import datetime

from scratches.scraper import scrape
from airflow.decorators import dag, task
from plugins.operators.influxdb_connector import InfluxDBWriteDictOperator

logging.getLogger().setLevel(logging.INFO)

@dag(schedule_interval= '0 * * * *', start_date=datetime(2022, 8, 31), catchup=False, render_template_as_native_obj=True)
def scrape_data_flow():
    @task(task_id='scraping_word', retires=2)
    def push_data():
        pass