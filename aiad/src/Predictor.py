# Copyright (c) 2023 Institute of Communication and Computer Systems
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.        

import datetime
import json
import threading
import time
import os, sys
import multiprocessing
import traceback
from subprocess import PIPE, run
from exn import core

import logging
from exn import connector
from exn.core.handler import Handler
from exn.handler.connector_handler import ConnectorHandler
from aiad.aiad import train_aiad, inference_aiad
from runtime.operational_status.ApplicationState import ApplicationState
#from runtime.predictions.Prediction import Prediction
from runtime.utilities.PredictionPublisher import PredictionPublisher
from runtime.utilities.Utilities import Utilities

from runtime.operational_status.AiadPredictorState import AiadPredictorState
import numpy as np

print_with_time = Utilities.print_with_time

   
def BuildingAModel(application_state, next_prediction_time):

    # Get the filename with the all metrics to obatin the model
    application_state.model_data_filename = application_state.get_model_allmetrics_filename(
        AiadPredictorState.configuration_file_location)

    if AiadPredictorState.testing_functionality:
        print_with_time("Testing mode. Building the model for the Anomaly Detector with the ./datasets/_ApplicationPaula-model2.csv file (without next_prediction_time).")
        scaler, myNSA = train_aiad(application_state.model_data_filename, application_state.lower_bound_value, application_state.upper_bound_value)
    else:
        print_with_time("Building the model for the Anomaly Detector with real data (with the next_prediction_time).")
        scaler, myNSA = train_aiad(application_state.model_data_filename, application_state.lower_bound_value, application_state.upper_bound_value,
                                               next_prediction_time=next_prediction_time)

    return scaler, myNSA

def TestingAModel(scaler, myNSA, application_state, next_prediction_time):

    # Get the filename with the all metrics to infer
    application_state.prediction_data_filename = application_state.get_prediction_allmetrics_filename(
        AiadPredictorState.configuration_file_location)

    start_time = time.time()

    if AiadPredictorState.testing_functionality:
        print_with_time("Testing mode. Inferring anomaly data with the ./datasets/_ApplicationPaula-test2.csv file (without next_prediction_time).")
        results = inference_aiad(scaler, myNSA, application_state.application_name, application_state.prediction_data_filename)
    else:
        print_with_time("Inferring anomaly data with with real data (with the next_prediction_time).")
        results = inference_aiad(scaler, myNSA, application_state.application_name, application_state.prediction_data_filename,
                                               next_prediction_time=next_prediction_time)

    if results is not None:
        results["last_prediction_time_needed"] = int(time.time() - start_time)

    return results

def update_prediction_time(epoch_start, prediction_horizon, maximum_time_for_prediction):
    current_time = time.time()
    prediction_intervals_since_epoch = ((current_time - epoch_start) // prediction_horizon)
    estimated_time_after_prediction = current_time + maximum_time_for_prediction
    earliest_time_to_predict_at = epoch_start + (
            prediction_intervals_since_epoch + 1) * prediction_horizon  # these predictions will concern the next prediction interval

    if (estimated_time_after_prediction > earliest_time_to_predict_at):
        future_prediction_time_factor = 1 + (
                estimated_time_after_prediction - earliest_time_to_predict_at) // prediction_horizon
        prediction_time = earliest_time_to_predict_at + future_prediction_time_factor * prediction_horizon
        print_with_time(
            "Due to slowness of the prediction, skipping next time point for prediction (prediction at " + str(
                earliest_time_to_predict_at - prediction_horizon) + " for " + str(
                earliest_time_to_predict_at) + ") and targeting " + str(
                future_prediction_time_factor) + " intervals ahead (prediction at time point " + str(
                prediction_time - prediction_horizon) + " for " + str(prediction_time) + ")")
    else:
        prediction_time = earliest_time_to_predict_at + prediction_horizon
    print_with_time(
        "Time is now " + str(current_time) + " and next prediction batch starts with prediction for time " + str(
            prediction_time))
    return prediction_time


def convert_to_native(obj):
    if isinstance(obj, (np.integer, np.int64)):
        return int(obj)
    elif isinstance(obj, (np.floating, np.float64)):
        return float(obj)
    elif isinstance(obj, np.ndarray):
        return obj.tolist()  # Convert arrays to lists
    elif isinstance(obj, dict):
        return {k: convert_to_native(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [convert_to_native(i) for i in obj]
    return obj


def calculate_and_publish_predictions(application_state, application_name, maximum_time_required_for_prediction):
    start_forecasting = application_state.start_forecasting

    while start_forecasting:
        print_with_time("Using " + AiadPredictorState.configuration_file_location + " for configuration details...")
        application_state.next_prediction_time = update_prediction_time(application_state.epoch_start,
                                                                        application_state.prediction_horizon,
                                                                        maximum_time_required_for_prediction)
                    
        if ((application_state.previous_prediction is not None) and (
                application_state.previous_prediction["last_prediction_time_needed"] > maximum_time_required_for_prediction)):
            maximum_time_required_for_prediction = application_state.previous_prediction["last_prediction_time_needed"]

        # Below we subtract one reconfiguration interval, as we cannot send a prediction for a time point later than one prediction_horizon interval
        wait_time = application_state.next_prediction_time - application_state.prediction_horizon - time.time()
        print_with_time("Waiting for " + str(
            (int(wait_time * 100)) / 100) + " seconds, until time " + datetime.datetime.fromtimestamp(
            application_state.next_prediction_time - application_state.prediction_horizon).strftime(
            '%Y-%m-%d %H:%M:%S'))
        if (wait_time > 0):
            time.sleep(wait_time)
            if (not start_forecasting):
                break

        Utilities.load_configuration()

        if not AiadPredictorState.testing_functionality:     # in testing mode a namefile is hard code
            there_is_data = application_state.update_model_data()       
        if not there_is_data:
            print_with_time("IMPORTANT: There is NO data to TRAIN/CREATE the model! Application name: " + application_name)
            wait_time = AiadPredictorState.number_of_minutes_to_infer * 2 * 60
            print_with_time("Waiting for " + str(AiadPredictorState.number_of_minutes_to_infer * 2) + " minutes before updating the model.")
            if (wait_time > 0):
                time.sleep(wait_time)
            
        else:
        
            first_prediction = None

            prediction_time = int(application_state.next_prediction_time)       
            scaler, myNSA = BuildingAModel(application_state, prediction_time)
            
            if scaler is None and myNSA is None:
                
                print_with_time("IMPORTANT: There is NO data to TRAIN/CREATE the model! Application name: " + application_name)
                
            else:
                
                #for prediction_index in range(0, AiadPredictorState.total_time_intervals_to_predict):
                prediction_index = 0
                while prediction_index < AiadPredictorState.total_time_intervals_to_predict and application_state.start_forecasting:
                    logging.info(f'Beginning cycle of {application_name} (index is {prediction_index} of total_time_intervals {AiadPredictorState.total_time_intervals_to_predict} *************')
                    prediction_time = int(
                        application_state.next_prediction_time) + prediction_index * application_state.prediction_horizon
                    try:
                        
                        prediction = None
                        
                        if not AiadPredictorState.testing_functionality:     # in testing mode a namefile is hard code
                            there_is_data = application_state.update_monitoring_data()

                        if not there_is_data:
                            print_with_time("IMPORTANT: There is NO data to MONITOR!")
                            wait_time = AiadPredictorState.number_of_minutes_to_infer * 60
                            print_with_time("Waiting for " + str(AiadPredictorState.number_of_minutes_to_infer) + " minutes before monitoring again.")
                            #wait_time = 60                                                                                  # ONLY FOR TESTING. PAULA
                            #print_with_time("Waiting for 60 SEGUNDOS before monitoring again. ESTO SE DEBE CAMBIAR!!!!!")   # ONLY FOR TESTING. PAULA
                            if (wait_time > 0):
                                time.sleep(wait_time)
                            break

                        print_with_time("Initiating predictions for all metrics for next_prediction_time, which is " + str(
                            application_state.next_prediction_time) + " prediction_index " + str(prediction_index))
                        
                        prediction = TestingAModel(scaler, myNSA, application_state, prediction_time)
                        if (prediction_time == int(application_state.next_prediction_time)):
                            first_prediction = prediction
                    except Exception as e:
                        print_with_time("Could not create a prediction for the metrics for time point " + str(
                            application_state.next_prediction_time) + ", proceeding to next prediction time. Index " + str(
                            prediction_index) + " of " + str(
                            AiadPredictorState.total_time_intervals_to_predict) + " configured intervals. The encountered exception trace follows:")
                        print(traceback.format_exc())
                        break

                    if (AiadPredictorState.disconnected or AiadPredictorState.check_stale_connection()):
                        logging.info("Possible problem due to disconnection or a stale connection")
                        # State.connection.connect()
                    
                    if prediction is not None:
                        if prediction["window_anomaly_rate"] >= AiadPredictorState.ai_nsa_anomaly_rate:
                            message_not_sent = True
                            current_time = int(time.time())
                            prediction_message_body = {
                                "method": "aiad",
                                "level": 3,
                                "application": application_name,
                                "timestamp": np.int64(current_time),
                                "window_start": np.int64(prediction["data"].index.min()),
                                "window_end": np.int64(prediction["data"].index.max()),
                                "window_anomaly_rate": prediction["window_anomaly_rate"],
                                "predictionTime": np.int64(prediction_time),
                                "metrics": application_state.metrics_to_predict
                            }
                            # Convert message to native types
                            prediction_message_body = convert_to_native(prediction_message_body)
                            while (message_not_sent):
                                try:
                                    # for publisher in State.broker_publishers:
                                    #    if publisher.
                                    for publisher in AiadPredictorState.broker_publishers:
                                        if publisher.key == "publisher_" + application_name + "-" + "allmetrics":
                                            publisher.send(prediction_message_body, application_name)
                                            print_with_time("publisher.send")

                                    message_not_sent = False
                                    print_with_time(
                                        "Successfully sent anomaly detection message for %s to topic eu.nebulouscloud.preliminary_predicted.%s.%s:\n\n%s\n\n" % (
                                            "allmetrics", AiadPredictorState.forecaster_name, "allmetrics", prediction_message_body))
                                except ConnectionError as exception:
                                    logging.error("Error sending an anomaly detection" + str(exception))
                                    AiadPredictorState.disconnected = False
                        else:
                            print_with_time("NO anomaly detection message sent for application " + application_name + ". Window anomaly rate is "+str(prediction["window_anomaly_rate"])+" (Send when is >= " +str(AiadPredictorState.ai_nsa_anomaly_rate)+ ").")
                            print_with_time("******************************************************************************************\n\n")
                        
                    logging.info(f'Ending cycle of {application_name} (index is {prediction_index} of total_time_intervals {AiadPredictorState.total_time_intervals_to_predict} *************')
                    prediction_index += 1
                
                if (first_prediction is not None):
                    application_state.previous_prediction = first_prediction  # first_prediction is the first of the batch of the predictions which are produced. The size of this batch is set by the State.total_time_intervals_to_predict (currently set to 12)
       
        if not application_state.start_forecasting:
            start_forecasting = application_state.start_forecasting
            logging.info(f'Ending the cycle --> a new monitoring data arrived at topic metric_list.')

# class Listener(messaging.listener.MorphemicListener):
class BootStrap(ConnectorHandler):
    pass


class ConsumerHandler(Handler):
    prediction_thread = {}

    def ready(self, context):
        if context.has_publisher('state'):
            context.publishers['state'].starting()
            context.publishers['state'].started()
            context.publishers['state'].custom('forecasting')
            context.publishers['state'].stopping()
            context.publishers['state'].stopped()

            # context.publishers['publisher_cpu_usage'].send({
            #     'hello': 'world'
            # })

    def on_message(self, key, address, body, context, **kwargs):
        address = address.replace("topic://" + AiadPredictorState.GENERAL_TOPIC_PREFIX, "")
        if (address).startswith(AiadPredictorState.MONITORING_DATA_PREFIX):
            address = address.replace(AiadPredictorState.MONITORING_DATA_PREFIX, "", 1)

            logging.info("New monitoring data arrived at topic " + address)
            if address == 'metric_list':
                application_name = body["name"]
                message_version = body["version"]
                logging.info("on_message body: " + str(body))
                application_state = None
                individual_application_state = {}
                application_already_defined = application_name in AiadPredictorState.individual_application_state
                if (application_already_defined and
                        (message_version == AiadPredictorState.individual_application_state[
                            application_name].message_version)
                ):
                    individual_application_state = AiadPredictorState.individual_application_state
                    application_state = individual_application_state[application_name]

                    print_with_time("Using existing application definition for " + application_name)
                else:
                    if (application_already_defined):
                        print_with_time(
                            "Updating application " + application_name + " based on new metrics list message")
                    else:
                        print_with_time("Creating new application " + application_name)
                    application_state = ApplicationState(application_name, message_version)
                    
                    
                if application_state.start_forecasting:
                    print_with_time("Stopping the anomaly detector for " + application_name + ". Changing start_forecasting to False to change metrics.")
                    application_state.start_forecasting = False

                    if application_name in self.prediction_thread:
                        thread = self.prediction_thread[application_name]
                        thread_ident = thread.ident
                        print_with_time(f'Blocking the execution of the thread for {application_name} until it has finished --> ident: {thread_ident}.')
                        thread.join()
                        print_with_time(f'Unblocking the execution of the thread for {application_name} --> ident: {thread_ident}.')
                        del self.prediction_thread[application_name]
                    
                metric_list_object = body["metric_list"]
                # Only take into account the list of metrics received since that moment.
                #lower_bound_value = application_state.lower_bound_value
                #upper_bound_value = application_state.upper_bound_value
                # End. Only take into account the list of metrics received since that moment.
                lower_bound_value = {}
                upper_bound_value = {}
                metrics_to_predict = []
                for metric_object in metric_list_object:
                    lower_bound_value[metric_object["name"]] = float(metric_object["lower_bound"])
                    upper_bound_value[metric_object["name"]] = float(metric_object["upper_bound"])
                    
                    metrics_to_predict.append(metric_object["name"])
                
                application_state.lower_bound_value.update(lower_bound_value)
                application_state.upper_bound_value.update(upper_bound_value)
                application_state.metrics_to_predict = metrics_to_predict

                application_state.initial_metric_list_received = True

                individual_application_state[application_name] = application_state
                AiadPredictorState.individual_application_state.update(individual_application_state)

                print(f'application_state.metrics_to_predict {application_state.metrics_to_predict}')
                print(f'application_name {application_name}')
                print_with_time("Starting the anomaly detector using the following metrics: " + ",".join(
                    application_state.metrics_to_predict) + " for application " + application_name + ", proceeding with the anomaly detector process")

                # Check if the publisher already exists
                already_exists = False
                for publisher in AiadPredictorState.broker_publishers:
                    if publisher.key == "publisher_" + application_name + "-" + "allmetrics":
                        already_exists = True
                if not already_exists:
                    AiadPredictorState.broker_publishers.append(PredictionPublisher(application_name, "allmetrics"))
                    print_with_time("Adding a new AiadPredictorState.broker_publishers for application name " + application_name)
                else:
                    print_with_time("Using an old AiadPredictorState.broker_publishers for application name " + application_name)
                AiadPredictorState.publishing_connector = connector.EXN(
                    'publishing_' + AiadPredictorState.forecaster_name + '-' + application_name,
                    handler=BootStrap(),
                    # consumers=list(State.broker_consumers),
                    consumers=[],
                    publishers=AiadPredictorState.broker_publishers,
                    url=AiadPredictorState.broker_address,
                    port=AiadPredictorState.broker_port,
                    username=AiadPredictorState.broker_username,
                    password=AiadPredictorState.broker_password
                )
                # AiadPredictorState.publishing_connector.start()
                thread = threading.Thread(target=AiadPredictorState.publishing_connector.start, args=())
                thread.start()
                
                
                

                application_state = AiadPredictorState.individual_application_state[application_name]
                application_state.start_forecasting = True
                #application_state.epoch_start = body["epoch_start"]
                #application_state.prediction_horizon = int(body["prediction_horizon"])
                application_state.next_prediction_time = update_prediction_time(application_state.epoch_start,
                                                                                application_state.prediction_horizon,
                                                                                AiadPredictorState.prediction_processing_time_safety_margin_seconds)  # State.next_prediction_time was assigned the value of State.epoch_start here, but this re-initializes targeted prediction times after each start_forecasting message, which is not desired necessarily
                print_with_time(
                    "A metric_list aiad message for " + application_name + "has been received, epoch start and prediction horizon are " + str(
                        application_state.epoch_start) + ", and " + str(
                        application_state.prediction_horizon) + " seconds respectively")
                        
                        
                        

                with open(AiadPredictorState.configuration_file_location, "r+b") as f:

                    AiadPredictorState.configuration_details.load(f, "utf-8")

                    # Do stuff with the p object...
                    initial_seconds_aggregation_value, metadata = AiadPredictorState.configuration_details[
                        "number_of_seconds_to_aggregate_on"]
                    initial_seconds_aggregation_value = int(initial_seconds_aggregation_value)

                    if (application_state.prediction_horizon < initial_seconds_aggregation_value):
                        print_with_time("Changing number_of_seconds_to_aggregate_on to " + str(
                            application_state.prediction_horizon) + " from its initial value " + str(
                            initial_seconds_aggregation_value))
                        AiadPredictorState.configuration_details["number_of_seconds_to_aggregate_on"] = str(
                            application_state.prediction_horizon)

                    f.seek(0)
                    f.truncate(0)
                    AiadPredictorState.configuration_details.store(f, encoding="utf-8")

                maximum_time_required_for_prediction = AiadPredictorState.prediction_processing_time_safety_margin_seconds  # initialization, assuming X seconds processing time to derive a first prediction
                
                
                if application_name not in self.prediction_thread or not self.prediction_thread[application_name].is_alive():
                    print_with_time("calculate_and_publish_predictions for " + application_name)
                    thread = threading.Thread(target=calculate_and_publish_predictions,
                                              args=[application_state, application_name, maximum_time_required_for_prediction])
                    thread.start()
                    self.prediction_thread[application_name] = thread
                    print_with_time(f'Thread ident: {thread.ident} for application {application_name} AFTER the start.')
                else:
                    print_with_time(f"Thread for {application_name} is already running.")
            
        else:
            print_with_time("Received message " + body + " but could not handle it")


def get_dataset_file(attribute):
    pass


def main():
    # Ensure the configuration file location is provided as a command-line argument
    if len(sys.argv) < 2:
        print("Error: Configuration file location must be provided as an argument.")
        sys.exit(1)

    # Set the configuration file location from the command-line argument
    configuration_file_location = sys.argv[1]
    AiadPredictorState.configuration_file_location = configuration_file_location

    # Print the current directory contents for debugging
    #print(f'os.listdir(".") {os.listdir(".")}')

    # Load configurations
    Utilities.load_configuration()
    Utilities.update_influxdb_organization_id()
    # Subscribe to retrieve the metrics which should be used

    id = "aiad"
    AiadPredictorState.disconnected = True

    while True:
        topics_to_subscribe = ["eu.nebulouscloud.monitoring.metric_list"] 
        # topics_to_subscribe = ["eu.nebulouscloud.monitoring.metric_list", 
                                # "eu.nebulouscloud.monitoring.realtime.>",
                                # "eu.nebulouscloud.forecasting.start_forecasting.aiad",
                                # "eu.nebulouscloud.forecasting.stop_forecasting.aiad"]
        current_consumers = []

        for topic in topics_to_subscribe:
            current_consumer = core.consumer.Consumer(key='monitoring_' + topic, address=topic, handler=ConsumerHandler(),
                                                      topic=True, fqdn=True)
            AiadPredictorState.broker_consumers.append(current_consumer)
            current_consumers.append(current_consumer)
        AiadPredictorState.subscribing_connector = connector.EXN(AiadPredictorState.forecaster_name, handler=BootStrap(),
                                                                 # consumers=list(State.broker_consumers),
                                                                 consumers=AiadPredictorState.broker_consumers,
                                                                 url=AiadPredictorState.broker_address,
                                                                 port=AiadPredictorState.broker_port,
                                                                 username=AiadPredictorState.broker_username,
                                                                 password=AiadPredictorState.broker_password
                                                                 )

        # connector.start()
        thread = threading.Thread(target=AiadPredictorState.subscribing_connector.start, args=())
        thread.start()
        AiadPredictorState.disconnected = False;

        print_with_time("Checking (EMS) broker connectivity state, possibly ready to start")
        if (AiadPredictorState.disconnected or AiadPredictorState.check_stale_connection()):
            try:
                # State.connection.disconnect() #required to avoid the already connected exception
                # State.connection.connect()
                AiadPredictorState.disconnected = True
                print_with_time("Possible problem in the connection")
            except Exception as e:
                print_with_time("Encountered exception while trying to connect to broker")
                print(traceback.format_exc())
                AiadPredictorState.disconnected = True
                time.sleep(5)
                continue
        AiadPredictorState.disconnection_handler.acquire()
        AiadPredictorState.disconnection_handler.wait()
        AiadPredictorState.disconnection_handler.release()

    # State.connector.stop()

if __name__ == "__main__":
    main()