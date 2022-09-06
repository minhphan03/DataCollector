import json
import pandas as pd
from typing import List
from influxdb_client import InfluxDBClient
from influxdb_client.client.write.point import Point
from influxdb_client.client.write_api import SYNCHRONOUS
from influxdb_client.client.flux_table import FluxTable

from airflow.hooks.base import BaseHook
from airflow.models import Connection

class InfluxDBConnectorHook(BaseHook):
    """
    Interact with InfluxDB.
    Performs a connection to InfluxDB and retrieves client.
    :param influxdb_conn_id: connection id of influxdb server`.
    """

    conn_name_attr = 'influxdb_conn_id'
    default_conn_name = 'influxdb_default'
    conn_type = 'influxdb'
    hook_name = 'Influxdb'

    def __init__(self, conn_id: str = default_conn_name, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.conn_id = conn_id
        self.client = None
        self.org_name = ''
    
    def get_uri(self, conn: Connection):
        """
        Function to add additional parameters to the URI
        based on SSL (Secure Sockets Layer) or other InfluxDB host requirements
        """