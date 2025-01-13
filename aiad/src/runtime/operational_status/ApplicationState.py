import logging
import time
import traceback

import requests
import json

from runtime.operational_status.AiadPredictorState import AiadPredictorState
from runtime.utilities.InfluxDBConnector import InfluxDBConnector
from runtime.utilities.Utilities import Utilities
from dateutil import parser

import os
import pandas as pd
from datetime import datetime, timedelta

class ApplicationState:

    #Forecaster variables

    def get_model_data_filename(self,configuration_file_location,metric_name):
        from jproperties import Properties
        p = Properties()
        with open(configuration_file_location, "rb") as f:
            p.load(f, "utf-8")
            path_to_datasets, metadata = p["path_to_datasets"]
            path_to_datasets = Utilities.fix_path_ending(path_to_datasets)
            return "" + str(path_to_datasets) + str(self.application_name) + "-model_"+metric_name+ ".csv"

    def get_prediction_data_filename(self,configuration_file_location,metric_name):
        from jproperties import Properties
        p = Properties()
        with open(configuration_file_location, "rb") as f:
            p.load(f, "utf-8")
            path_to_datasets, metadata = p["path_to_datasets"]
            path_to_datasets = Utilities.fix_path_ending(path_to_datasets)
            return "" + str(path_to_datasets) + str(self.application_name) + "_"+metric_name+ ".csv"

    def get_model_allmetrics_filename(self,configuration_file_location):
        from jproperties import Properties
        p = Properties()
        with open(configuration_file_location, "rb") as f:
            p.load(f, "utf-8")
            path_to_datasets, metadata = p["path_to_datasets"]
            path_to_datasets = Utilities.fix_path_ending(path_to_datasets)
            return "" + str(path_to_datasets) + str(self.application_name) + "-model.csv"

    def get_prediction_allmetrics_filename(self,configuration_file_location):
        from jproperties import Properties
        p = Properties()
        with open(configuration_file_location, "rb") as f:
            p.load(f, "utf-8")
            path_to_datasets, metadata = p["path_to_datasets"]
            path_to_datasets = Utilities.fix_path_ending(path_to_datasets)
            return "" + str(path_to_datasets) + str(self.application_name) + ".csv"

    def get_prediction_path_to_datasets(self,configuration_file_location):
        from jproperties import Properties
        p = Properties()
        with open(configuration_file_location, "rb") as f:
            p.load(f, "utf-8")
            path_to_datasets, metadata = p["path_to_datasets"]
            path_to_datasets = Utilities.fix_path_ending(path_to_datasets)
            return "" + str(path_to_datasets)

    def __init__(self,application_name, message_version):
        self.message_version = message_version
        self.application_name = application_name
        self.influxdb_bucket = AiadPredictorState.application_name_prefix+application_name+"_bucket"
        token = AiadPredictorState.influxdb_token

        # PCF example en prod http://localhost:8086/api/v2/buckets?name=nebulous_73862382-92eb-482e-82f6-3b05c006b3af_bucket
        list_bucket_url = 'http://' + AiadPredictorState.influxdb_hostname + ':8086/api/v2/buckets?name='+self.influxdb_bucket
        create_bucket_url = 'http://' + AiadPredictorState.influxdb_hostname + ':8086/api/v2/buckets'
        headers = {
            'Authorization': 'Token {}'.format(token),
            'Content-Type': 'application/json',
            'Accept': 'application/json'
        }
        data = {
            'name': self.influxdb_bucket,
            'orgID': AiadPredictorState.influxdb_organization_id,
            'retentionRules': [
                {
                    'type': 'expire',
                    'everySeconds': 2592000 #30 days (30*24*3600)
                }
            ]
        }

        response = requests.get(list_bucket_url, headers=headers)

        logging.info("The response for listing a possibly existing bucket is "+str(response.status_code)+" for application "+application_name)
        if ((response.status_code==200) and ("buckets" in response.json()) and (len(response.json()["buckets"])>0)):
                logging.info("The bucket already existed for the particular application, skipping its creation...")
        else:
            logging.info("The response in the request to list a bucket is "+str(response.json()))
            logging.info("The bucket did not exist for the particular application, creation in process...")
            response = requests.post(create_bucket_url, headers=headers, data=json.dumps(data))
            logging.info("The response for creating a new bucket is "+str(response.status_code))
        self.start_forecasting = False  # Whether the component should start (or keep on) forecasting
        self.model_data_filename = application_name+"-model.csv"
        self.prediction_data_filename = application_name+".csv"
        self.dataset_file_name = "aiad_dataset_"+application_name+".csv"
        self.metrics_to_predict = []
        self.epoch_start = 0
        self.next_prediction_time = 0
        self.prediction_horizon = 120
        self.previous_prediction = None
        self.initial_metric_list_received = False
        self.lower_bound_value = {}
        self.upper_bound_value = {}


    def update_model_data(self):
        #query(metrics_to_predict,number_of_days_for_which_data_was_retrieved)
        #save_new_file()
        Utilities.print_with_time("Starting dataset creation process (update_model_data)...")
        try:
            for metric_name in self.metrics_to_predict:

                print_data_from_db = True
                
                current_time = time.time()
                end_time_offset = timedelta(minutes=AiadPredictorState.number_of_minutes_to_infer)          # Exclude last X minutes
                end_time = current_time - end_time_offset.total_seconds()                                   # IMPORTANT: Move the end time
                start_time = end_time - AiadPredictorState.number_of_days_to_use_data_from * 24 * 60 * 60   # Last X days since adjusted end
               
                # Convert timestamps to datetime objects
                start_time_dt = datetime.utcfromtimestamp(start_time)  # Convertir inicio a datetime
                end_time_dt = datetime.utcfromtimestamp(end_time)  # Convertir fin a datetime
               
                # Convert to ISO 8601 format
                start_time_iso = start_time_dt.isoformat() + "Z"  # Añadir 'Z' para indicar UTC
                end_time_iso = end_time_dt.isoformat() + "Z"
    
                query_string = (
                    f'from(bucket: "{self.influxdb_bucket}") '
                    f'|> range(start: {start_time_iso}, stop: {end_time_iso}) '
                    f'|> filter(fn: (r) => r["_measurement"] == "{metric_name}")'
                )
                
                influx_connector = InfluxDBConnector()
                logging.info("performing query for application with bucket "+str(self.influxdb_bucket))
                logging.info("The body of the query is "+query_string)
                logging.info("The configuration of the client is "+Utilities.get_fields_and_values(influx_connector))
                result = influx_connector.client.query_api().query(query_string, AiadPredictorState.influxdb_organization)
                
                elapsed_time = time.time()-current_time
                model_dataset_filename = self.get_model_data_filename(AiadPredictorState.configuration_file_location, metric_name)
                if len(result)>0:
                    logging.info(f"Performed query to the database, it took "+str(elapsed_time) + f" seconds to receive {len(result[0].records)} entries (from the first and possibly only table returned). Now logging to {model_dataset_filename}")
                    with open(model_dataset_filename, 'w') as file:
                        for table in result:
                            #print header row
                            file.write("Timestamp,ems_time,"+metric_name+"\r\n")
                            for record in table.records:
                                dt = parser.isoparse(str(record.get_time()))
                                epoch_time = int(dt.timestamp())
                                metric_value = record.get_value()
                                if(print_data_from_db):
                                    file.write(str(epoch_time)+","+str(epoch_time)+","+str(metric_value)+"\r\n")
                                    # Write the string data to the file
                else:
                    logging.info("No records were returned from the database to CREATE the model.")
                    there_is_data = False
                    return there_is_data
                    

            # Save all metrics in one file
            model_alldataset_filename = self.get_model_allmetrics_filename(AiadPredictorState.configuration_file_location)

            merged_df = pd.DataFrame()
            for metric_name in self.metrics_to_predict:
                model_dataset_filename = self.get_model_data_filename(AiadPredictorState.configuration_file_location, metric_name)
                
                # Read CSV file
                data = pd.read_csv(model_dataset_filename)
                
                # Extract the metric column name from the file (assuming it is the third column... the last column)
                metric_name = data.columns[-1]
                
                # Set 'ems_time' as index and convert it to datetime
                data.set_index('ems_time', inplace=True)
                data.index = pd.to_datetime(data.index, unit='s')
                
                # Resample at 1 second intervals and fill values ​​by interpolation
                data = data.resample('1s').mean().interpolate()
                
                # Reset index
                data.reset_index(inplace=True)
                
                # Rename the metric column
                data.rename(columns={metric_name: metric_name}, inplace=True)
                
                # Merge the data into the main DataFrame
                if merged_df.empty:
                    merged_df = data
                else:
                    merged_df = pd.merge(merged_df, data, on=['Timestamp', 'ems_time'], how='outer')

            # Sort by 'ems_time'
            merged_df.sort_values(by='ems_time', inplace=True)
            
            merged_df['Timestamp'] = merged_df['Timestamp'].astype(int)
            merged_df['ems_time'] = merged_df['Timestamp']
            
            # Determine columns with metrics (excluding 'Timestamp' and 'ems_time')
            value_columns = [col for col in merged_df.columns if col not in ['Timestamp', 'ems_time']]

            # Remove rows where any of the values ​​are NaN or negative
            merged_df = merged_df[~((merged_df[value_columns] < 0).any(axis=1) | merged_df[value_columns].isna().any(axis=1))]

            # Reorder columns
            columns_order = ['Timestamp', 'ems_time'] + [col for col in merged_df.columns if col not in ['Timestamp', 'ems_time']]
            merged_df = merged_df[columns_order]

            # Save the result in a single CSV file
            merged_df.to_csv(model_alldataset_filename, index=False)

            print(f"Combined file saved in: {model_alldataset_filename} update_model_data")
                
            there_is_data = True
            return there_is_data                

        except Exception as e:
            Utilities.print_with_time("Could not create new dataset as an exception was thrown")
            print(traceback.format_exc())


    def update_monitoring_data(self):
        #query(metrics_to_predict,number_of_days_for_which_data_was_retrieved)
        #save_new_file()
        Utilities.print_with_time("Starting dataset creation process (update_monitoring_data)...")

        try:
            for metric_name in self.metrics_to_predict:
                try:
                
                    print_data_from_db = True
       
                    time_interval = timedelta(minutes=AiadPredictorState.number_of_minutes_to_infer)    # Last X minutes... now 180 minutes
                    current_time = time.time()
                    start_time = current_time - time_interval.total_seconds()                           # Now - Last X minutes
                    end_time = current_time                                                             # Now

                    # Convertir timestamps a objetos datetime
                    start_time_dt = datetime.utcfromtimestamp(start_time)  # Convertir inicio a datetime
                    end_time_dt = datetime.utcfromtimestamp(end_time)  # Convertir fin a datetime
                   
                    # Convertir a formato ISO 8601
                    start_time_iso = start_time_dt.isoformat() + "Z"  # Añadir 'Z' para indicar UTC
                    end_time_iso = end_time_dt.isoformat() + "Z"
                    
                    query_string = (
                        f'from(bucket: "{self.influxdb_bucket}") '
                        f'|> range(start: {start_time_iso}, stop: {end_time_iso}) '
                        f'|> filter(fn: (r) => r["_measurement"] == "{metric_name}")'
                    )

                    influx_connector = InfluxDBConnector()
                    logging.info("performing query for application with bucket "+str(self.influxdb_bucket))
                    logging.info("The body of the query is "+query_string)
                    logging.info("The configuration of the client is "+Utilities.get_fields_and_values(influx_connector))
                    result = influx_connector.client.query_api().query(query_string, AiadPredictorState.influxdb_organization)
                    elapsed_time = time.time()-current_time
                    prediction_dataset_filename = self.get_prediction_data_filename(AiadPredictorState.configuration_file_location, metric_name)
                    if len(result)>0:
                        logging.info(f"Performed query to the database, it took "+str(elapsed_time) + f" seconds to receive {len(result[0].records)} entries (from the first and possibly only table returned). Now logging to {prediction_dataset_filename}")
                        with open(prediction_dataset_filename, 'w') as file:
                            for table in result:
                                #print header row
                                file.write("Timestamp,ems_time,"+metric_name+"\r\n")
                                #file.write("ems_time,"+metric_name+"\r\n")
                                for record in table.records:
                                    dt = parser.isoparse(str(record.get_time()))
                                    epoch_time = int(dt.timestamp())
                                    metric_value = record.get_value()
                                    if(print_data_from_db):
                                        file.write(str(epoch_time)+","+str(epoch_time)+","+str(metric_value)+"\r\n")
                                        #file.write(str(epoch_time)+","+str(metric_value)+"\r\n")
                                        # Write the string data to the file
                    else:
                        logging.info(f"No records were returned from the database for MONITORING (metric {metric_name})")
                        there_is_data = False
                        return there_is_data
                        
                except Exception as metric_error:
                    logging.error(f"An error occurred with metric '{metric_name}' between {start_time_iso} and {end_time_iso}: {metric_error}")
                    logging.error(traceback.format_exc())
                    there_is_data = False
                    return there_is_data
            
            # Save all metrics in one file
            prediction_alldataset_filename = self.get_prediction_allmetrics_filename(AiadPredictorState.configuration_file_location)

            merged_df = pd.DataFrame()
            for metric_name in self.metrics_to_predict:

                prediction_dataset_filename = self.get_prediction_data_filename(AiadPredictorState.configuration_file_location, metric_name)

                # Read CSV file
                print(f'prediction_dataset_filename update_monitoring_data {prediction_dataset_filename}')
                data = pd.read_csv(prediction_dataset_filename)
                
                # Extract the metric column name from the file (assuming it is the third column... the last column)
                metric_name = data.columns[-1]
                
                # Set 'ems_time' as index and convert it to datetime
                data.set_index('ems_time', inplace=True)
                data.index = pd.to_datetime(data.index, unit='s')
                
                # Resample at 1 second intervals and fill values ​​by interpolation
                data = data.resample('1s').mean().interpolate()
                
                # Reset index
                data.reset_index(inplace=True)
                
                # Rename the metric column
                data.rename(columns={metric_name: metric_name}, inplace=True)
                
                # Merge the data into the main DataFrame
                if merged_df.empty:
                    merged_df = data
                else:
                    merged_df = pd.merge(merged_df, data, on=['Timestamp', 'ems_time'], how='outer')

            # Sort by 'ems_time'
            merged_df.sort_values(by='ems_time', inplace=True)
            
            merged_df['Timestamp'] = merged_df['Timestamp'].astype(int)
            merged_df['ems_time'] = merged_df['Timestamp']
            
            # Determine columns with metrics (excluding 'Timestamp' and 'ems_time')
            value_columns = [col for col in merged_df.columns if col not in ['Timestamp', 'ems_time']]

            # Remove rows where any of the values ​​are NaN or negative
            merged_df = merged_df[~((merged_df[value_columns] < 0).any(axis=1) | merged_df[value_columns].isna().any(axis=1))]

            # Reorder columns
            columns_order = ['Timestamp', 'ems_time'] + [col for col in merged_df.columns if col not in ['Timestamp', 'ems_time']]
            merged_df = merged_df[columns_order]

            # Save the result in a single CSV file
            merged_df.to_csv(prediction_alldataset_filename, index=False)

            print(f"Combined file saved in: {prediction_alldataset_filename} update_monitoring_data")
            there_is_data = True

            return there_is_data


        except Exception as e:
            Utilities.print_with_time("Could not create new dataset as an exception was thrown")
            print(traceback.format_exc())
            there_is_data = False
            return there_is_data