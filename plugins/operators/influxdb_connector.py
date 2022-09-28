import json
from typing import Any, Dict
from airflow.utils.decorators import apply_defaults
from airflow.utils.context import Context
from airflow.models import BaseOperator
from hooks.influxdb_connector import InfluxDBConnectorHook

class InfluxDBQueryOperator(BaseOperator):
    """
    Executes query code in a specific InfluxDB instance:
    :param query: the query string
    :param influxdb_conn_id: connection id of influxdb server`.
    """
    template_fields = [
        'query',
        'influxdb_conn_id'
    ]

    template_fields_renderers = {
        'query': 'str',
        'influxdb_conn_id': 'str'
    }

    @apply_defaults
    def __init__(
        self,
        query: str,
        influxdb_conn_id: str = 'influxdb_default',
        **kwargs
    ) -> None:
        super().__init__(**kwargs)
        self.influxdb_conn_id = influxdb_conn_id
        self.query = query
    
    def execute(self, context: Context) -> Any:
        self.log.info('Executing: %s', self.query)
        self.hook = InfluxDBConnectorHook(conn_id=self.influxdb_conn_id)
        self.hook.query(self.query)

class InfluxDBQueryDataFrameOperator(InfluxDBQueryOperator):
    """
    Executes QUERYING code in a specific InfluxDB database
    :param query: the query string
    :param influxdb_conn_id: connection id of influxdb server`.
    """

    def execute(self, context: Context) -> Any:
        self.log.info('Executing: %s', self.query)
        self.hook = InfluxDBConnectorHook(conn_id=self.influxdb_conn_id)
        self.hook.query_data_frame(self.query)
    

class InfluxDBWriteDictOperator(BaseOperator):
    """
    Executes WRITING data in a specific InfluxDB database
    :param bucket_name: bucket to store data
    :param data: data to write into influxDB in dictionary
    :param influxdb_conn_id: connection id of influxdb server`.
    """
    template_fields = [
        'bucket_name',
        'data',
        'influxdb_conn_id'
    ]

    template_fields_renderers = {
        'bucket_name': 'str',
        'data': 'dict',
        'influxdb_conn_id': 'str'
    }

    @apply_defaults
    def __init__(
        self,
        bucket_name: str,
        data: Any,
        influxdb_conn_id: str = 'influxdb_default',
        **kwargs
    ) -> None:
        super().__init__(**kwargs)
        self.influxdb_conn_id = influxdb_conn_id
        self.bucket_name = bucket_name
        self.data = data
    
    def execute(self, context: Context) -> Any:
        self.hook = InfluxDBConnectorHook(conn_id=self.influxdb_conn_id)
        self.hook.write_dict(self.bucket_name, self.data)
