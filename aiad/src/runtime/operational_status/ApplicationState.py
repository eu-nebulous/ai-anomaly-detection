import logging
import time
import traceback
import requests
import json
from runtime.operational_status.AiadPredictorState import AiadPredictorState
from runtime.utilities.InfluxDBConnector import InfluxDBConnector
from runtime.utilities.Utilities import Utilities
from dateutil import parser
from datetime import datetime, timedelta
import pandas as pd
from influxdb_client.rest import ApiException


MAX_RETRIES = 5  # Número máximo de reintentos
RETRY_DELAY = 5  # Retraso inicial entre reintentos en segundos

class ApplicationState:

    def __init__(self, application_name, message_version):
        self.message_version = message_version
        self.application_name = application_name
        self.influxdb_bucket = AiadPredictorState.application_name_prefix + application_name + "_bucket"
        self.start_forecasting = False  # Whether the component should start (or keep on) forecasting
        self.model_data_filename = f"{application_name}-model.csv"
        self.prediction_data_filename = f"{application_name}.csv"
        self.dataset_file_name = f"aiad_dataset_{application_name}.csv"
        self.metrics_to_predict = []
        self.epoch_start = 0
        self.next_prediction_time = 0
        self.prediction_horizon = 300       # Time interval (in seconds) between consecutive predictions
        self.previous_prediction = None
        self.initial_metric_list_received = False
        self.lower_bound_value = {}
        self.upper_bound_value = {}

        self._ensure_bucket_exists()        
        
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
        return self._get_filename(configuration_file_location, "-model", metric_name)

    def get_prediction_data_filename(self, configuration_file_location, metric_name):
        return self._get_filename(configuration_file_location, "", metric_name)

    def get_model_allmetrics_filename(self, configuration_file_location):
        return self._get_filename(configuration_file_location, "-model")

    def get_prediction_allmetrics_filename(self, configuration_file_location):
        return self._get_filename(configuration_file_location, "")

    def update_model_data(self):
        Utilities.print_with_time("Starting dataset creation process (update_model_data)...")
        try:
            for metric_name in self.metrics_to_predict:
                retry_count = 0
                success = False
                while retry_count < MAX_RETRIES and not success:
                    try:
                        current_time = time.time()
                        result = self._query_influxdb(metric_name, past_days=AiadPredictorState.number_of_days_to_use_data_from)
                        if result:
                            elapsed_time = time.time() - current_time
                            model_dataset_filename = self.get_model_data_filename(AiadPredictorState.configuration_file_location, metric_name)
                            logging.info(f"Performed query to the database, it took " + str(elapsed_time) + f" seconds to receive {len(result[0].records)} entries. Now logging to {model_dataset_filename}")

                            self._save_to_csv(result, model_dataset_filename, metric_name)
                            success = True
                        else:
                            logging.info(f"No data returned for metric {metric_name}.")
                            return False
                    except TimeoutError as e:
                        retry_count += 1
                        if retry_count < MAX_RETRIES:
                            logging.error(f"Unexpected error occurred for metric {metric_name}: {str(e)}. Retrying {retry_count}/{MAX_RETRIES}...............................")
                            time.sleep(RETRY_DELAY * (2 ** (retry_count - 1)))  # Retraso exponencial
                        else:
                            logging.error(f"Maximum retries reached for metric {metric_name}.")
                            return None
                    except Exception as e:
                        logging.error(f"Module update_model_data. Unexpected error while querying metric {metric_name}: {str(e)}")
                        return None
                if not success:
                    # Si no se tuvo éxito después de los reintentos, retornar error
                    logging.error(f"Failed to process metric {metric_name} after {retry_count} retries.")
                    return None                      
            return self._merge_datasets(self.get_model_data_filename, self.get_model_allmetrics_filename(AiadPredictorState.configuration_file_location))
        except Exception as e:
            Utilities.print_with_time("Error in update_model_data")
            logging.error(traceback.format_exc())
            return None

    def update_monitoring_data(self):
        Utilities.print_with_time("Starting dataset creation process (update_monitoring_data)...")
        try:
            for metric_name in self.metrics_to_predict:
                retry_count = 0
                success = False
                while retry_count < MAX_RETRIES and not success:
                    try:
                        current_time = time.time()
                        result = self._query_influxdb(metric_name, past_minutes=AiadPredictorState.number_of_minutes_to_infer)
                        if result:
                            elapsed_time = time.time() - current_time
                            monitor_dataset_filename = self.get_prediction_data_filename(AiadPredictorState.configuration_file_location, metric_name)
                            logging.info(f"Performed query to the database, it took "+str(elapsed_time) + f" seconds to receive {len(result[0].records)}. Now logging to {monitor_dataset_filename}")

                            self._save_to_csv(result, monitor_dataset_filename, metric_name)
                            success = True
                        else:
                            logging.info(f"No data returned for metric {metric_name}.")
                            return False
                    except TimeoutError as e:
                        retry_count += 1
                        if retry_count < MAX_RETRIES:
                            logging.error(f"Module update_monitoring_data. Unexpected error occurred for metric {metric_name}: {str(e)}. Retrying {retry_count}/{MAX_RETRIES}...............................")
                            time.sleep(RETRY_DELAY * (2 ** (retry_count - 1)))  # Retraso exponencial
                        else:
                            logging.error(f"Maximum retries reached for metric {metric_name}.")
                            return None
                    except Exception as e:
                        logging.error(f"Unexpected error while querying metric {metric_name}: {str(e)}")
                        return None
                if not success:
                    # Si no se tuvo éxito después de los reintentos, retornar error
                    logging.error(f"Failed to process metric {metric_name} after {retry_count} retries.")
                    return None
            return self._merge_datasets(self.get_prediction_data_filename, self.get_prediction_allmetrics_filename(AiadPredictorState.configuration_file_location))
        except Exception as e:
            Utilities.print_with_time("Error in update_monitoring_data")
            logging.error(traceback.format_exc())
            return None

    def _query_influxdbBack(self, metric_name, past_days=0, past_minutes=0):
        current_time = time.time()
        start_time = current_time - (past_days * 24 * 60 * 60) - (past_minutes * 60)
        end_time = current_time
        start_time_iso = datetime.utcfromtimestamp(start_time).isoformat() + "Z"
        end_time_iso = datetime.utcfromtimestamp(end_time).isoformat() + "Z"

        query_string = (
            f'from(bucket: "{self.influxdb_bucket}") '
            f'|> range(start: {start_time_iso}, stop: {end_time_iso}) '
            f'|> filter(fn: (r) => r["_measurement"] == "{metric_name}")'
        )
        influx_connector = InfluxDBConnector()
        logging.info(f"Performing query: {query_string}")
        return influx_connector.client.query_api().query(query_string, AiadPredictorState.influxdb_organization)

    def _query_influxdb(self, metric_name, past_days=0, past_minutes=0):
        current_time = time.time()
        start_time = current_time - (past_days * 24 * 60 * 60) - (past_minutes * 60)
        end_time = current_time
        start_time_iso = datetime.utcfromtimestamp(start_time).isoformat() + "Z"
        end_time_iso = datetime.utcfromtimestamp(end_time).isoformat() + "Z"

        query_string = (
            f'from(bucket: "{self.influxdb_bucket}") '
            f'|> range(start: {start_time_iso}, stop: {end_time_iso}) '
            f'|> filter(fn: (r) => r["_measurement"] == "{metric_name}")'
        )

        influx_connector = InfluxDBConnector()
        logging.info(f"Performing query: {query_string}")

        try:
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

    def _save_to_csv(self, result, filename, metric_name):
        with open(filename, "w") as file:
            file.write("Timestamp,ems_time,"+metric_name+"\r\n")
            for table in result:
                for record in table.records:
                    dt = parser.isoparse(str(record.get_time()))
                    epoch_time = int(dt.timestamp())
                    metric_value = record.get_value()
                    file.write(f"{epoch_time},{epoch_time},{metric_value}\n")
        logging.info(f"Data saved to {filename}")

    def _merge_datasets(self, get_filename_func, output_filename):
        merged_df = pd.DataFrame()
        for metric_name in self.metrics_to_predict:
            # Read CSV file
            data = pd.read_csv(get_filename_func(AiadPredictorState.configuration_file_location, metric_name))
            
            # Extract the metric column name from the file (assuming it is the third column... the last column)
            metric_name2 = data.columns[-1]
            
            # Set 'ems_time' as index and convert it to datetime
            data.set_index("ems_time", inplace=True)
            data.index = pd.to_datetime(data.index, unit="s")
            
            # Resample at 1 second intervals and fill values ​​by interpolation
            data = data.resample('1s').mean().interpolate()
            
            # Reset index
            data.reset_index(inplace=True)
            
            # Rename the metric column
            data.rename(columns={metric_name: metric_name2}, inplace=True)            
            
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
        merged_df = merged_df[~((merged_df[metric_columns] < 0).any(axis=1) | merged_df[metric_columns].isna().any(axis=1))]

        # Reorder columns
        merged_df = merged_df[['Timestamp', 'ems_time'] + metric_columns]

        # Save the result in a single CSV file
        merged_df.to_csv(output_filename, index=False)
        logging.info(f"Combined data saved to {output_filename}")
        return True
