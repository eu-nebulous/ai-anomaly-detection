from influxdb_client import InfluxDBClient, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS
import logging
from runtime.operational_status.AiadPredictorState import AiadPredictorState

class InfluxDBConnector:

    def __init__(self, timeout=30000):
        self.client = InfluxDBClient(url="http://" + AiadPredictorState.influxdb_hostname + ":" + str(AiadPredictorState.influxdb_port), token=AiadPredictorState.influxdb_token, org=AiadPredictorState.influxdb_organization, timeout=timeout)
        self.write_api = self.client.write_api(write_options=SYNCHRONOUS)
        # logging.info("Successfully created InfluxDB connector, client is "+str(self.client))
    def write_data(self,data,bucket):
        self.write_api.write(bucket=bucket, org=AiadPredictorState.influxdb_organization, record=data, write_precision=WritePrecision.S)

    def get_data(self):
        query_api = self.client.query_api()
        query = """from(bucket: "nebulous")
         |> range(start: -1m)
         |> filter(fn: (r) => r._measurement == "temperature")"""
        tables = query_api.query(query, org=AiadPredictorState.influxdb_organization)

        for table in tables:
            for record in table.records:
                print(record)

    # el close es NUEVO PCF... ver qué hacer
    def close(self):
        try:
            self.client.close()
            # logging.info("InfluxDB client closed successfully.")
        except Exception as e:
            logging.warning(f"Error closing InfluxDB client: {e}")
