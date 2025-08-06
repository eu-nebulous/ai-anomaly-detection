# import logging
# from runtime.utilities.InfluxDBConnector import InfluxDBConnector

# class MetricDiscovery:
    # def __init__(self, influxdb_bucket, influxdb_organization):
        # self.influxdb_bucket = influxdb_bucket
        # self.influxdb_organization = influxdb_organization

    # def get_allowed_metrics(self, instance):
        # query_string = (
            # f'from(bucket: "{self.influxdb_bucket}") '
            # f'|> range(start: -3h) '
            # f'|> filter(fn: (r) => r["_measurement"] =~ /^adt_/ and r["instance"] == "{instance}") '
            # f'|> keep(columns: ["_measurement", "destination_key"]) '
            # f'|> group(columns: ["_measurement", "destination_key"]) '
            # f'|> distinct()'
        # )

        # logging.info(f"[MetricDiscovery] Querying allowed_metrics:\n{query_string}")
        # influx_connector = None
        # try:
            # influx_connector = InfluxDBConnector()
            # tables = influx_connector.client.query_api().query(query_string, self.influxdb_organization)
            # metrics = {
                # (
                    # record.values["_measurement"],
                    # (
                        # destination_key_raw.replace(", ", "---")
                        # if ", " in destination_key_raw else
                        # destination_key_raw.replace(",", "--")
                        # if "," in destination_key_raw else
                        # destination_key_raw
                    # )
                # )
                # for table in tables
                # for record in table.records
                # if "_measurement" in record.values and "destination_key" in record.values
                # for destination_key_raw in [record.values["destination_key"]]
            # }            
            # return sorted(metrics)

        # except Exception as e:
            # logging.error(f"[MetricDiscovery] Error retrieving allowed metrics: {type(e).__name__}: {e}")

        # finally:
            # try:
                # influx_connector.close()
            # except Exception as e:
                # logging.warning(f"Error closing InfluxDB client: {e}")

        # return []

import logging
import time
import requests
import urllib3
from runtime.utilities.InfluxDBConnector import InfluxDBConnector

MAX_RETRIES = 3
RETRY_DELAY = 2  # segundos

class MetricDiscovery:
    def __init__(self, influxdb_bucket, influxdb_organization):
        self.influxdb_bucket = influxdb_bucket
        self.influxdb_organization = influxdb_organization

    def get_allowed_metrics(self, instance):
        query_string = (
            f'from(bucket: "{self.influxdb_bucket}") '
            f'|> range(start: -3h) '
            f'|> filter(fn: (r) => r["_measurement"] =~ /^adt_/ and r["instance"] == "{instance}") '
            f'|> keep(columns: ["_measurement", "destination_key"]) '
            f'|> group(columns: ["_measurement", "destination_key"]) '
            f'|> distinct()'
        )

        logging.info(f"[MetricDiscovery] Querying allowed_metrics:\n{query_string}")

        retry_count = 0
        while retry_count < MAX_RETRIES:
            influx_connector = None
            try:
                influx_connector = InfluxDBConnector(timeout=30000)  # 30 segundos
                tables = influx_connector.client.query_api().query(query_string, self.influxdb_organization)
                metrics = {
                    (
                        record.values["_measurement"],
                        (
                            destination_key_raw.replace(", ", "---")
                            if ", " in destination_key_raw else
                            destination_key_raw.replace(",", "--")
                            if "," in destination_key_raw else
                            destination_key_raw
                        )
                    )
                    for table in tables
                    for record in table.records
                    if "_measurement" in record.values and "destination_key" in record.values
                    for destination_key_raw in [record.values["destination_key"]]
                }
                return sorted(metrics)

            except (requests.exceptions.ReadTimeout, requests.exceptions.Timeout, urllib3.exceptions.ReadTimeoutError) as e:
                retry_count += 1
                logging.error(f"[MetricDiscovery] Timeout error: {e}. Retrying {retry_count}/{MAX_RETRIES}...")
                time.sleep(RETRY_DELAY * (2 ** (retry_count - 1)))

            except Exception as e:
                retry_count += 1
                logging.error(f"[MetricDiscovery] Error retrieving allowed metrics: {type(e).__name__}: {e}. Retrying {retry_count}/{MAX_RETRIES}...")
                time.sleep(RETRY_DELAY * (2 ** (retry_count - 1)))

            finally:
                if influx_connector is not None:
                    try:
                        influx_connector.close()
                    except Exception as e:
                        logging.warning(f"Error closing InfluxDB client: {e}")

        logging.error("[MetricDiscovery] Maximum retries reached. Returning empty list.")
        return []
