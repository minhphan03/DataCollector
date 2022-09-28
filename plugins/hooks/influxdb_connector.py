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
        Function to add additional parameters to the URI (uniform resource identifier)
        based on SSL (Secure Sockets Layer) or other InfluxDB host requirements
        """
        conn_schema = conn.schema
        conn_port = conn.port
        uri = conn.host

        # check if there is a default schema to stick in
        if conn_schema:
            uri = f'{conn_schema}://{uri}'
        if conn_port:
            uri = f'{uri}:{conn_port}'
        return uri
    
    def get_conn(self) -> InfluxDBClient:
        """
        Function that initiates a new InfluxDB connection
        with token and organization name
        """
        # if connection is already established, return the connection
        if self.client:
            return self.client
        
        conn = self.get_connection(self.conn_id)
        extras = conn.extra_dejson.copy()
        uri = self.get_uri(conn)
        token = extras['token']

        self.org_name = extras['org_name']
        self.client = InfluxDBClient(url=uri, token=token, org=self.org_name)
        self.log.info(f'InfluxDB uri={uri}')
        self.log.info(f'InfluxDB org={self.org_name}')
        
        return self.client
    
    def write_point(self, bucket_name: str, point: Point, synchronous=True) -> None:
        """
        Write a point to influxdb
        """
        client = self.get_conn()

        # By default it's batching (writing multiple points in one instance)
        # with synchronous = True 
        if synchronous:
            write_api = client.write_api(write_options=SYNCHRONOUS)
        else:
            write_api = client.write_api()
        
        write_api.write(bucket=bucket_name, org=self.org_name, record=point)

    def write_dict(self, bucket_name: str, data: dict, synchronous=True):
        """
        Write a dict to influxdb
        """
        self.log.info('Write Point to database data=%s', json.dumps(data))
        point = Point.from_dict(data)
        self.write_point(bucket_name=bucket_name, point=point, synchronous=synchronous)
    
    def query(self, query: str, params: dict = None) -> List[FluxTable]:
        """
        Function to to run the query.
        Note: The bucket name should be included in the query
        :param query: InfluxDB query
        :param params: bind parameter
        :return: List
        """
        client = self.get_conn()
        query_api = client.query_api()
        return query_api.query(query, params=params)
    
    def query_data_frame(self, query: str, params: dict = None) -> pd.DataFrame:
        """
        Function to run the query and return a pandas dataframe
        Note: The bucket name should be included in the query
        :param query: InfluxDB query
        :param params: bind parameter
        :return: pd.DataFrame
        """
        client = self.get_conn()
        query_api = client.query_api()
        return query_api.query_data_frame(query, params=params)
    
    def fetch_data(
        self,
        bucket: str,
        measurement: str,
        index: str,
        size: int,
        keep_column: List[str]
    ) -> pd.DataFrame:
        """
        Get data from influxDB in data frame format
        """
        params = {
            '_bucket': bucket,
            '_measurement': measurement,
            '_index': index,
            '_size': size,
            '_columns': keep_column,
        }

        # query in InfluxDB language
        query = '''
            from(bucket: _bucket)
                |> range(start: -90d)
                |> filter(fn: (r) => r["_measurement"] == _measurement)
                |> filter(fn: (r) => r["index"] == _index)
                |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
                |> keep(columns: _columns)
                |> sort(columns: ["_time"], desc: true)
                |> limit(n: _size)
                |> sort(columns: ["_time"], desc: false)
            '''

        data = self.query_data_frame(query, params)
        print(data.head)
        return data