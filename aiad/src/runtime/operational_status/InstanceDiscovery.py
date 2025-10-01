# import logging
# from runtime.utilities.InfluxDBConnector import InfluxDBConnector

# class InstanceDiscovery:
    # def __init__(self, influxdb_bucket, influxdb_organization):
        # self.influxdb_bucket = influxdb_bucket
        # self.influxdb_organization = influxdb_organization

    # def get_all_instances(self):
        # query_string = (
            # f'from(bucket: "{self.influxdb_bucket}") '
            # f'|> range(start: -3h) '
            # f'|> filter(fn: (r) => r["_measurement"] =~ /^adt_/) '
            # f'|> keep(columns: ["instance"]) '
            # f'|> distinct(column: "instance")'
        # )

        # logging.info(f"[InstanceDiscovery] Querying InfluxDB for distinct instances:\n{query_string}")
        
        # try:
            # influx_connector = InfluxDBConnector()
            # tables = influx_connector.client.query_api().query(query_string, self.influxdb_organization)
            # instances = set()

            # for table in tables:
                # for record in table.records:
                    # instance_value = record.get_value()
                    # if instance_value:
                        # instances.add(instance_value)

            # return list(instances)

        # except Exception as e:
            # logging.error(f"[InstanceDiscovery] Error retrieving instances from InfluxDB: {e}")
            # return []

import logging
import time
import requests
import urllib3
from runtime.utilities.InfluxDBConnector import InfluxDBConnector

MAX_RETRIES = 3
RETRY_DELAY = 2  # seconds

class InstanceDiscovery:
    def __init__(self, influxdb_bucket, influxdb_organization):
        self.influxdb_bucket = influxdb_bucket
        self.influxdb_organization = influxdb_organization

    def get_all_instances(self, number_of_days_to_use_data_from):
        query_string = (
            f'from(bucket: "{self.influxdb_bucket}") '
            f'|> range(start: -{number_of_days_to_use_data_from}d) '
            f'|> filter(fn: (r) => r["_measurement"] =~ /^adt_/) '
            f'|> keep(columns: ["instance"]) '
            f'|> distinct(column: "instance")'
            f'|> limit(n: 10)'
        )
        # query_string = (
            # f'from(bucket: "{self.influxdb_bucket}") '
            # f'|> range(start: -{number_of_days_to_use_data_from}d) '
            # f'|> filter(fn: (r) => r["_measurement"] =~ /^adt_/) '
            # f'|> distinct(column: "instance")'
        # )

        logging.info(f"[InstanceDiscovery] Querying InfluxDB for different instances over the last {number_of_days_to_use_data_from} days:\n{query_string}")

        retry_count = 0
        while retry_count < MAX_RETRIES:
            influx_connector = None
            try:
                influx_connector = InfluxDBConnector()
                tables = influx_connector.client.query_api().query(query_string, self.influxdb_organization)
                instances = set()

                for table in tables:
                    for record in table.records:
                        instance_value = record.get_value()
                        if instance_value:
                            instances.add(instance_value)

                logging.info(f"[InstanceDiscovery] Instances {instances}.")
                return list(instances)

            except (requests.exceptions.ReadTimeout, requests.exceptions.Timeout, urllib3.exceptions.ReadTimeoutError) as e:
                retry_count += 1
                logging.error(f"[InstanceDiscovery] Timeout error: {e}. Retrying {retry_count}/{MAX_RETRIES}...")
                time.sleep(RETRY_DELAY * (2 ** (retry_count - 1)))  # exponential backoff

            except Exception as e:
                retry_count += 1
                logging.error(f"[InstanceDiscovery] Error retrieving instances from InfluxDB: {e}. Retrying {retry_count}/{MAX_RETRIES}...")
                time.sleep(RETRY_DELAY * (2 ** (retry_count - 1)))

            finally:
                try:
                    influx_connector.close()
                except Exception as e:
                    logging.warning(f"Error closing InfluxDB client: {e}")

        logging.error("[InstanceDiscovery] Maximum retries reached. Returning empty list.")
        return []
