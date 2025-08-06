import logging
import time
import traceback
import requests
import urllib3
import json
from runtime.operational_status.AiadPredictorState import AiadPredictorState
from runtime.utilities.InfluxDBConnector import InfluxDBConnector
from runtime.utilities.Utilities import Utilities
from dateutil import parser
from datetime import datetime, timedelta
import pandas as pd
from influxdb_client.rest import ApiException
from runtime.operational_status.MetricDiscovery import MetricDiscovery

MAX_RETRIES = 5  # Número máximo de reintentos
RETRY_DELAY = 5  # Retraso inicial entre reintentos en segundos

class ApplicationState:

    def __init__(self, application_name, message_version, instance):
        self.message_version = message_version
        self.application_name = application_name
        self.instance = instance
        self.influxdb_bucket = AiadPredictorState.application_name_prefix + application_name + "_bucket"
        self.start_forecasting = True
        self.model_data_filename = f"{application_name}-model.csv"
        self.prediction_data_filename = f"{application_name}.csv"
        self.dataset_file_name = f"aiad_dataset_{application_name}.csv"
        self.metrics_to_predict = []
        self.epoch_start = 0
        self.next_prediction_time = 0
        self.prediction_horizon = 300       # Time interval (in seconds) between consecutive predictions
        #self.prediction_horizon = 120       # To testing. Time interval (in seconds) between consecutive predictions
        self.previous_prediction = None
        self.initial_metric_list_received = False
        self.lower_bound_value = {}
        self.upper_bound_value = {}

        self._ensure_bucket_exists()

        # Obtain metrics dynamically
        discovery = MetricDiscovery(self.influxdb_bucket, AiadPredictorState.influxdb_organization)
        self.allowed_metrics = discovery.get_allowed_metrics(instance)
        logging.info(f"[ApplicationState] allowed_metrics for {application_name}@{instance}: {self.allowed_metrics}")

        # self.allowed_metrics = [
            # ("adt_ipv4_errors", "OutNoRoutes"),
            # ("adt_ipv4_packets", "delivered"),
            # ("adt_ipv4_packets", "forwarded"),
            # ("adt_ipv4_packets", "received"),
            # ("adt_ipv4_packets", "sent"),
            # ("adt_ipv6_errors", "OutNoRoutes"),
            # ("adt_ipv6_packets", "delivered"),
            # ("adt_ipv6_packets", "received"),
            # ("adt_ipv6_packets", "sent"),
            # ("adt_net_errors", "inbound"),
            # ("adt_net_events", "frames"),
            # ("adt_net_net", "received"),
            # ("adt_net_net", "sent"),
            # ("adt_net_packets", "received"),
            # ("adt_net_packets", "sent"),
        # ]        
        # self.first_instance = self._get_first_instance()
        # logging.info(f"First instance detected: {self.first_instance}")
        # self.all_instances = self._get_all_instances()     
        # logging.info(f"All instances detected: {self.all_instances}")

    # def _get_first_instance(self):  
        # query_string = (
            # f'from(bucket: "{self.influxdb_bucket}") '
            # f'|> range(start: 0) '
            # f'|> filter(fn: (r) => r["_measurement"] =~ /^adt_/)'
            # f'|> keep(columns: ["instance"])'
            # f'|> distinct(column: "instance")'
        # )

        # logging.info(f"Performing query to obtain an 'instance': {query_string}")
        
        # influx_connector = InfluxDBConnector()
        # try:
            # tables = influx_connector.client.query_api().query(query_string, AiadPredictorState.influxdb_organization)
            # first_instance = None
            # for table in tables:
                # for record in table.records:
                    # first_instance = record['instance']
                    # break  # Exit after getting the first 'instance' record
                # if first_instance:
                    # break  # Exit after getting the first 'instance' record
            # return first_instance
        # except Exception as e:
            # logging.error(f"Error getting first instance: {e}")
        # return None
        
    # def _get_all_instances(self):
        # query_string = (
            # f'from(bucket: "{self.influxdb_bucket}") '
            # f'|> range(start: 0) '
            # f'|> filter(fn: (r) => r["_measurement"] =~ /^adt_/) '
            # f'|> keep(columns: ["instance"]) '
            # f'|> distinct(column: "instance")'
        # )

        # logging.info(f"Performing query to obtain all 'instance' values: {query_string}")
        
        # influx_connector = InfluxDBConnector()
        # try:
            # tables = influx_connector.client.query_api().query(query_string, AiadPredictorState.influxdb_organization)
            # instances = []
            # for table in tables:
                # for record in table.records:
                    # instance_value = record.get_value()
                    # if instance_value not in instances:
                        # instances.append(instance_value)
            # return instances
        # except Exception as e:
            # logging.error(f"Error getting instances: {e}")
            # return []    
        
    def _ensure_bucket_exists(self):
        token = AiadPredictorState.influxdb_token
        list_bucket_url = f"http://{AiadPredictorState.influxdb_hostname}:8086/api/v2/buckets?name={self.influxdb_bucket}"
        create_bucket_url = f"http://{AiadPredictorState.influxdb_hostname}:8086/api/v2/buckets"
        headers = {
            "Authorization": f"Token {token}",
            "Content-Type": "application/json",
            "Accept": "application/json",
        }
        data = {
            "name": self.influxdb_bucket,
            "orgID": AiadPredictorState.influxdb_organization_id,
            "retentionRules": [{"type": "expire", "everySeconds": 2592000}],  # (30 days * 24 hours * 60 minutes * 60 seconds)
        }
        response = requests.get(list_bucket_url, headers=headers)
        logging.info("The response for listing a possibly existing bucket is " + str(response.status_code) + " for application " + self.application_name)
        if (
            response.status_code == 200
            and "buckets" in response.json()
            and len(response.json()["buckets"]) > 0
        ):
            logging.info(f"Bucket already exists for application {self.application_name}, skipping its creation...")
        else:
            logging.info("The response in the request to list a bucket is " + str(response.json()))
            logging.info(f"Creating bucket for application {self.application_name}")
            response = requests.post(create_bucket_url, headers=headers, data=json.dumps(data))
            logging.info("The response for creating a new bucket is " + str(response.status_code))

    def _get_path_from_config(self, configuration_file_location):
        from jproperties import Properties

        p = Properties()
        with open(configuration_file_location, "rb") as f:
            p.load(f, "utf-8")
            path_to_datasets, _ = p["path_to_datasets"]
            return Utilities.fix_path_ending(path_to_datasets)

    def _get_filename(self, configuration_file_location, suffix, metric_name=""):
        path_to_datasets = self._get_path_from_config(configuration_file_location)
        return f"{path_to_datasets}{self.application_name}{suffix}_{metric_name}.csv"

    def get_model_data_filename(self, configuration_file_location, metric_name):
        return self._get_filename(configuration_file_location, f"-{self.instance}-model", metric_name)

    def get_prediction_data_filename(self, configuration_file_location, metric_name):
        return self._get_filename(configuration_file_location, f"-{self.instance}", metric_name)

    def get_model_allmetrics_filename(self, configuration_file_location):
        return self._get_filename(configuration_file_location, f"-{self.instance}-model")

    def get_prediction_allmetrics_filename(self, configuration_file_location):
        return self._get_filename(configuration_file_location, f"-{self.instance}")

    def update_model_data(self):
        Utilities.print_with_time("Starting dataset creation process (update_model_data)...")
        try:
            #for metric_name in self.metrics_to_predict:
            allowed_metrics_aux = self.allowed_metrics.copy()
            for metric_name, destination_key in self.allowed_metrics:
                retry_count = 0
                success = False
                while retry_count < MAX_RETRIES and not success:
                    try:
                        current_time = time.time()
                        result = self._query_influxdb(True, metric_name, destination_key, past_days=AiadPredictorState.number_of_days_to_use_data_from, past_minutes=AiadPredictorState.number_of_minutes_to_infer)
                        #metric_destination = f"{self.instance}_{metric_name}_{destination_key}"
                        metric_destination = f"{metric_name}_{destination_key}"
                        #if result:
                        total_records = sum(len(table.records) for table in result)
                        if total_records > AiadPredictorState.min_number_of_records_to_model:
                            elapsed_time = time.time() - current_time
                            model_dataset_filename = self.get_model_data_filename(AiadPredictorState.configuration_file_location, metric_destination)
                            logging.info(f"Performed query to the database, it took " + str(elapsed_time) + f" seconds to receive {total_records} entries. Now logging to {model_dataset_filename}")

                            self._save_to_csv(result, model_dataset_filename, metric_destination)
                            success = True
                        else:
                            if result:
                                logging.info(f"[update_model_data] Only {total_records} records received for instance {self.instance} metric {metric_destination}. We wait more than {AiadPredictorState.min_number_of_records_to_model} records.")
                            else:
                                logging.info(f"[update_model_data] No data returned for instance {self.instance} metric {metric_destination}.")
                            logging.info(f'[update_model_data] Deleting metric_name {metric_name} destination_key {destination_key}')
                            allowed_metrics_aux.remove((metric_name, destination_key))
                            success = True
                    except TimeoutError as e:
                        retry_count += 1
                        if retry_count < MAX_RETRIES:
                            logging.error(f"Module update_model_data. Unexpected error occurred for metric {metric_name}: {str(e)}. Retrying {retry_count}/{MAX_RETRIES}...............................")
                            time.sleep(RETRY_DELAY * (2 ** (retry_count - 1)))  # Retraso exponencial
                        else:
                            logging.error(f"Maximum retries reached for metric {metric_name}.")
                            return None
                    except Exception as e:
                        logging.error(f"Module update_model_data. Unexpected error while querying metric {metric_name}: {str(e)}")
                        return None
                if not success:
                    # If unsuccessful after retries, return an error
                    logging.error(f"Failed to process metric {metric_name} after {retry_count} retries.")
                    return None
            self.allowed_metrics = allowed_metrics_aux
            if not self.allowed_metrics:
                return None
            return self._merge_datasets(self.get_model_data_filename, self.get_model_allmetrics_filename(AiadPredictorState.configuration_file_location))
        except Exception as e:
            Utilities.print_with_time("Error in update_model_data")
            logging.error(traceback.format_exc())
            return None

    def update_monitoring_data(self):
        Utilities.print_with_time("Starting dataset creation process (update_monitoring_data)...")
        try:
            #for metric_name in self.metrics_to_predict:
            allowed_metrics_aux = self.allowed_metrics.copy()
            for metric_name, destination_key in self.allowed_metrics:
                retry_count = 0
                success = False
                while retry_count < MAX_RETRIES and not success:
                    try:
                        current_time = time.time()
                        result = self._query_influxdb(False, metric_name, destination_key, past_minutes=AiadPredictorState.number_of_minutes_to_infer)
                        #metric_destination = f"{self.instance}_{metric_name}_{destination_key}"
                        metric_destination = f"{metric_name}_{destination_key}"
                        total_records = sum(len(table.records) for table in result)
                        if result:
                            elapsed_time = time.time() - current_time
                            monitor_dataset_filename = self.get_prediction_data_filename(AiadPredictorState.configuration_file_location, metric_destination)
                            logging.info(f"Performed query to the database, it took "+str(elapsed_time) + f" seconds to receive {total_records}. Now logging to {monitor_dataset_filename}")

                            self._save_to_csv(result, monitor_dataset_filename, metric_destination)
                            success = True
                        else:
                            logging.info(f"[update_monitoring_data] No data returned for instance {self.instance} metric {metric_destination}.")
                            logging.info(f'[update_monitoring_data] Deleting metric_name {metric_name} destination_key {destination_key}')
                            allowed_metrics_aux.remove((metric_name, destination_key))
                            success = True
                    except (requests.exceptions.ReadTimeout, requests.exceptions.Timeout, urllib3.exceptions.ReadTimeoutError) as e:
                    #except TimeoutError as e:
                        retry_count += 1
                        if retry_count < MAX_RETRIES:
                            logging.error(f"[update_monitoring_data] Timeout error for metric {metric_name}: {str(e)}. Retrying {retry_count}/{MAX_RETRIES}...............................")
                            #logging.error(f"[update_monitoring_data] Unexpected error occurred for metric {metric_name}: {str(e)}. Retrying {retry_count}/{MAX_RETRIES}...............................")
                            time.sleep(RETRY_DELAY * (2 ** (retry_count - 1)))  # Retraso exponencial
                        else:
                            logging.error(f"[update_monitoring_data] Timeout error for metric {metric_name}: {str(e)}. Maximum retries reached.")
                            return None
                    except Exception as e:
                        logging.error(f"[update_monitoring_data] Unexpected error while querying metric {metric_name}: {str(e)}")
                        return None
                if not success:
                    # If unsuccessful after retries, return an error
                    logging.error(f"[update_monitoring_data] Failed to process metric {metric_name} after {retry_count} retries.")
                    return None
            self.allowed_metrics = allowed_metrics_aux
            return self._merge_datasets(self.get_prediction_data_filename, self.get_prediction_allmetrics_filename(AiadPredictorState.configuration_file_location))
        except Exception as e:
            Utilities.print_with_time("[update_monitoring_data] Error in update_monitoring_data module.")
            logging.error(traceback.format_exc())
            return None

    def _query_influxdbBack(self, model_data, metric_name, destination_key, past_days=0, past_minutes=0):
        current_time = time.time()
        if model_data:
            start_time = current_time - (past_days * 24 * 60 * 60) - (past_minutes * 60)
            #end_time = current_time - (past_minutes * 60)      # DEBERIA QUEDAR ESTO ESPERANDO QUE EXISTAN DATOS ANTES DE PREDECIR
            end_time = current_time                             # ELIMINAR ESTO PCF
        else:
            start_time = current_time - (past_minutes * 60)
            end_time = current_time
        start_time_iso = datetime.utcfromtimestamp(start_time).isoformat() + "Z"
        end_time_iso = datetime.utcfromtimestamp(end_time).isoformat() + "Z"

        destination_key_orig = destination_key
        destination_key_orig = destination_key_orig.replace("---", ", ")
        destination_key_orig = destination_key_orig.replace("--", ",")
        query_string = (
            f'from(bucket: "{self.influxdb_bucket}") '
            f'|> range(start: {start_time_iso}, stop: {end_time_iso}) '
            f'|> filter(fn: (r) => r["_measurement"] == "{metric_name}" and r["instance"] == "{self.instance}" and r["destination_key"] == "{destination_key_orig}")'
        )

        influx_connector = None
        try:
            influx_connector = InfluxDBConnector()
            logging.info(f"Performing query: {query_string}")
            # Try to query InfluxDB
            return influx_connector.client.query_api().query(query_string, AiadPredictorState.influxdb_organization)
        except ApiException as e:
            # Specifically handle ApiException exception
            if e.status == 404:
                error_message = e.body.decode('utf-8') if hasattr(e.body, 'decode') else e.body
                logging.error(f"Bucket not found: {self.influxdb_bucket}. Details: {error_message}")
                raise ValueError(f"Bucket '{self.influxdb_bucket}' does not exist. Please check the configuration.") from e
            else:
                logging.error(f"API exception occurred: {str(e)}")
                raise
        except Exception as e:
            # Catch any other exceptions
            logging.error(f"Unexpected error: {str(e)}")
            raise
        finally:
            influx_connector.close()  

    def _query_influxdb(self, model_data, metric_name, destination_key, past_days=0, past_minutes=0):
        current_time = time.time()
        if model_data:
            start_time = current_time - (past_days * 24 * 60 * 60) - (past_minutes * 60)
            #end_time = current_time - (past_minutes * 60)      # DEBERIA QUEDAR ESTO ESPERANDO QUE EXISTAN DATOS ANTES DE PREDECIR
            end_time = current_time                             # ELIMINAR ESTO PCF
        else:
            start_time = current_time - (past_minutes * 60)
            end_time = current_time

        start_time_iso = datetime.utcfromtimestamp(start_time).isoformat() + "Z"
        end_time_iso = datetime.utcfromtimestamp(end_time).isoformat() + "Z"

        destination_key_orig = destination_key.replace("---", ", ").replace("--", ",")
        query_string = (
            f'from(bucket: "{self.influxdb_bucket}") '
            f'|> range(start: {start_time_iso}, stop: {end_time_iso}) '
            f'|> filter(fn: (r) => r["_measurement"] == "{metric_name}" and r["instance"] == "{self.instance}" and r["destination_key"] == "{destination_key_orig}")'
        )

        retry_count = 0
        while retry_count < MAX_RETRIES:
            influx_connector = None
            try:
                influx_connector = InfluxDBConnector(timeout=30000)  # 30 segundos
                logging.info(f"Performing query: {query_string}")
                return influx_connector.client.query_api().query(query_string, AiadPredictorState.influxdb_organization)

            except (requests.exceptions.ReadTimeout, requests.exceptions.Timeout, urllib3.exceptions.ReadTimeoutError) as e:
                retry_count += 1
                logging.error(f"[InfluxDB] Timeout error: {e}. Retrying {retry_count}/{MAX_RETRIES}...")
                time.sleep(RETRY_DELAY * (2 ** (retry_count - 1)))

            except ApiException as e:
                if e.status == 404:
                    error_message = e.body.decode('utf-8') if hasattr(e.body, 'decode') else e.body
                    logging.error(f"Bucket not found: {self.influxdb_bucket}. Details: {error_message}")
                    raise ValueError(f"Bucket '{self.influxdb_bucket}' does not exist. Please check the configuration.") from e
                else:
                    retry_count += 1
                    logging.error(f"[InfluxDB] API exception occurred: {str(e)}. Retrying {retry_count}/{MAX_RETRIES}...")
                    time.sleep(RETRY_DELAY * (2 ** (retry_count - 1)))

            except Exception as e:
                retry_count += 1
                logging.error(f"[InfluxDB] Unexpected error: {str(e)}. Retrying {retry_count}/{MAX_RETRIES}...")
                time.sleep(RETRY_DELAY * (2 ** (retry_count - 1)))

            finally:
                if influx_connector is not None:
                    try:
                        influx_connector.close()
                    except Exception as e:
                        logging.warning(f"Error closing InfluxDB client: {e}")

        logging.error(f"[InfluxDB] Maximum retries reached for metric {metric_name}.")
        return None


    def _save_to_csv(self, result, filename, metric_destination):
        with open(filename, "w") as file:
            file.write("Timestamp,ems_time,"+metric_destination+"\r\n")
            for table in result:
                for record in table.records:
                    dt = parser.isoparse(str(record.get_time()))
                    epoch_time = int(dt.timestamp())
                    metric_value = record.get_value()
                    file.write(f"{epoch_time},{epoch_time},{metric_value}\n")
        logging.info(f"Data saved to {filename}")

    def _merge_datasets(self, get_filename_func, output_filename):
        merged_df = pd.DataFrame()
        #for metric_name in self.metrics_to_predict:
        for metric_name, destination_key in self.allowed_metrics:
            #metric_destination = f"{self.instance}_{metric_name}_{destination_key}"
            metric_destination = f"{metric_name}_{destination_key}"

            # Read CSV file
            data = pd.read_csv(get_filename_func(AiadPredictorState.configuration_file_location, metric_destination))
            
            # Extract the metric column name from the file (assuming it is the third column... the last column)
            metric_name2 = data.columns[-1]
            
            # Set 'ems_time' as index and convert it to datetime
            data.set_index("ems_time", inplace=True)
            data.index = pd.to_datetime(data.index, unit="s")
            
            # Resample at 1 second intervals and fill values ​​by interpolation
            data = data.resample('1s').mean().interpolate()
            
            # Reset index
            data.reset_index(inplace=True)
            
            # Rename the metric column. There is no need to rename metric_destination to metric_name2 since they are the same!!!
            # data.rename(columns={metric_destination: metric_name2}, inplace=True)
            
            # Merge the data into the main DataFrame
            if merged_df.empty:
                merged_df = data
            else:
                merged_df = pd.merge(merged_df, data, on=["Timestamp", "ems_time"], how="outer")
                
        # Sort by 'ems_time'
        merged_df.sort_values(by='ems_time', inplace=True)
        
        merged_df['Timestamp'] = merged_df['Timestamp'].astype(int)
        merged_df['ems_time'] = merged_df['Timestamp']
        
        # Determine columns with metrics (excluding 'Timestamp' and 'ems_time')
        metric_columns = [col for col in merged_df.columns if col not in ['Timestamp', 'ems_time']]

        # Remove rows where any of the values ​​are NaN or negative
        # merged_df = merged_df[~((merged_df[metric_columns] < 0).any(axis=1) | merged_df[metric_columns].isna().any(axis=1))]
        # Remove rows where any of the values ​​are NaN
        merged_df = merged_df[~merged_df[metric_columns].isna().any(axis=1)]


        # Reorder columns
        merged_df = merged_df[['Timestamp', 'ems_time'] + metric_columns]

        # Save the result in a single CSV file
        merged_df.to_csv(output_filename, index=False)
        logging.info(f"Combined data saved to {output_filename}")
        return True
