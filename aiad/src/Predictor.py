import datetime
import json
import threading
import time
import os, sys
import pandas as pd
import multiprocessing
import traceback
from subprocess import PIPE, run
from exn import core

import logging
from exn import connector
from exn.core.handler import Handler
from exn.handler.connector_handler import ConnectorHandler
from aiad.aiad import train_aiad, inference_aiad
from proton import Message
from runtime.operational_status.ApplicationState import ApplicationState
from runtime.operational_status.InstanceDiscovery import InstanceDiscovery
#from runtime.predictions.Prediction import Prediction
from runtime.utilities.PredictionPublisher import PredictionPublisher
from runtime.utilities.Utilities import Utilities

from runtime.operational_status.AiadPredictorState import AiadPredictorState
import numpy as np
from influxdb_client import InfluxDBClient

print_with_time = Utilities.print_with_time

   
def BuildingAModel(application_state, next_prediction_time):

    if AiadPredictorState.testing_functionality:
        print_with_time("Testing mode. Building the model for the Anomaly Detector with the ./datasets/_ApplicationPaula-model2.csv file.")
        application_state.model_data_filename = './datasets/_ApplicationPaula-model2.csv'
    else:
        # print_with_time("Building the model for the Anomaly Detector with real data.")
        # Get the filename with the all metrics to obatin the model
        application_state.model_data_filename = application_state.get_model_allmetrics_filename(AiadPredictorState.configuration_file_location)
        
    columns_with_variability, scaler, myNSA, myKmeans = train_aiad(AiadPredictorState.ai_nsa, AiadPredictorState.ai_kmeans, application_state.model_data_filename, application_state.lower_bound_value, application_state.upper_bound_value)

    return columns_with_variability, scaler, myNSA, myKmeans

def TestingAModel(columns_with_variability, scaler, myNSA, myKmeans, application_state, next_prediction_time):

    start_time = time.time()

    if AiadPredictorState.testing_functionality:
        print_with_time("Testing mode. Inferring anomaly data with the ./datasets/_ApplicationPaula-test2.csv file.")
        application_state.prediction_data_filename = './datasets/_ApplicationPaula-test2.csv'
    else:
        # print_with_time("Inferring anomaly data with with real data.")
        # Get the filename with the all metrics to infer
        application_state.prediction_data_filename = application_state.get_prediction_allmetrics_filename(AiadPredictorState.configuration_file_location)
        
    results = inference_aiad(AiadPredictorState.ai_nsa, AiadPredictorState.ai_kmeans, columns_with_variability, scaler, myNSA, myKmeans, application_state.prediction_data_filename, application_state.application_name)

    if results is not None:
        results["last_prediction_time_needed"] = int(time.time() - start_time)

    return results

def update_prediction_time(epoch_start, prediction_horizon, maximum_time_for_prediction):
    print(f"epoch_start {epoch_start} prediction_horizon {prediction_horizon} maximum_time_for_prediction {maximum_time_for_prediction}")
    current_time = time.time()
    prediction_intervals_since_epoch = ((current_time - epoch_start) // prediction_horizon)
    estimated_time_after_prediction = current_time + maximum_time_for_prediction
    earliest_time_to_predict_at = epoch_start + (
            prediction_intervals_since_epoch + 1) * prediction_horizon  # these predictions will concern the next prediction interval

    print(f"current_time {current_time} prediction_intervals_since_epoch {prediction_intervals_since_epoch} estimated_time_after_prediction {estimated_time_after_prediction} earliest_time_to_predict_at {earliest_time_to_predict_at}")

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
    elif isinstance(obj, (np.ndarray, pd.Index)):
        return obj.tolist()  # Convert arrays to lists
    elif isinstance(obj, dict):
        return {k: convert_to_native(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [convert_to_native(i) for i in obj]
    return obj

def calculate_and_publish_predictionsBack(application_state, app_instance_id, maximum_time_required_for_prediction, prediction_thread):
    
    logging.info(f"We are in calculate_and_publish_predictions: application_state {application_state} app_instance_id {app_instance_id} application_state.start_forecasting {application_state.start_forecasting}")
    
    application_state.start_forecasting
    
    there_is_modeling_data = False
    there_is_monitoring_data = False
    max_retries_modeling_data = 2
    retries_modeling_data = 0
    
    while application_state.start_forecasting and (AiadPredictorState.ai_nsa or AiadPredictorState.ai_kmeans):
        print_with_time("Using " + AiadPredictorState.configuration_file_location + " for configuration details...")

        if ((application_state.previous_prediction is not None) and (
                application_state.previous_prediction["last_prediction_time_needed"] > maximum_time_required_for_prediction)):
            maximum_time_required_for_prediction = application_state.previous_prediction["last_prediction_time_needed"]

        application_state.next_prediction_time = update_prediction_time(application_state.epoch_start,
                                                                        application_state.prediction_horizon,
                                                                        maximum_time_required_for_prediction)

        # Below we subtract one reconfiguration interval, as we cannot send a prediction for a time point later than one prediction_horizon interval
        wait_time = application_state.next_prediction_time - application_state.prediction_horizon - time.time()
        print_with_time("Waiting for " + str(
            (int(wait_time * 100)) / 100) + " seconds, until time " + datetime.datetime.fromtimestamp(
            application_state.next_prediction_time - application_state.prediction_horizon).strftime(
            '%Y-%m-%d %H:%M:%S'))
        if (wait_time > 0):
            time.sleep(wait_time)

        Utilities.load_configuration()

        if not AiadPredictorState.testing_functionality:     # in testing mode a namefile is hard code
            there_is_modeling_data = application_state.update_model_data()
        else:
            there_is_modeling_data = True
        if there_is_modeling_data is not None and not there_is_modeling_data:
            print_with_time("IMPORTANT: There is NO data to TRAIN/CREATE the model! Application name: " + application_state.application_name)
            wait_time = AiadPredictorState.number_of_minutes_to_infer * 2 * 60
            print_with_time("AiadPredictorState.number_of_minutes_to_infer " + str(AiadPredictorState.number_of_minutes_to_infer))
            print_with_time("Waiting for " + str(AiadPredictorState.number_of_minutes_to_infer * 2) + " minutes before creating / updating the model.")
            if (wait_time > 0):
                time.sleep(wait_time)
            
        elif there_is_modeling_data is not None and there_is_modeling_data:

            columns_with_variability, scaler, myNSA, myKmeans = BuildingAModel(application_state, int(application_state.next_prediction_time))
            
            if scaler is None and myNSA is None and myKmeans is None:
                
                print_with_time("IMPORTANT: There is NO data to TRAIN/CREATE the model! Application name: " + application_state.application_name)
                
            else:
                
                prediction_index = 0
                while prediction_index < AiadPredictorState.total_time_intervals_to_predict and application_state.start_forecasting:
                    logging.info(f'Beginning cycle of {application_state.application_name} (index is {prediction_index} of total_time_intervals {AiadPredictorState.total_time_intervals_to_predict} *************')
                    
                    if ((application_state.previous_prediction is not None) and (
                            application_state.previous_prediction["last_prediction_time_needed"] > maximum_time_required_for_prediction)):
                        maximum_time_required_for_prediction = application_state.previous_prediction["last_prediction_time_needed"]


                    application_state.next_prediction_time = update_prediction_time(application_state.epoch_start,
                                                                                    application_state.prediction_horizon,
                                                                                    maximum_time_required_for_prediction)

                    # Below we subtract one reconfiguration interval, as we cannot send a prediction for a time point later than one prediction_horizon interval
                    wait_time = application_state.next_prediction_time - application_state.prediction_horizon - time.time()
                    print_with_time("Waiting for " + str(
                        (int(wait_time * 100)) / 100) + " seconds, until time " + datetime.datetime.fromtimestamp(
                        application_state.next_prediction_time - application_state.prediction_horizon).strftime(
                        '%Y-%m-%d %H:%M:%S'))
                    if (wait_time > 0):
                        time.sleep(wait_time)
                    
                    try:
                        
                        prediction = None
                        there_is_monitoring_data = False
                        
                        if not AiadPredictorState.testing_functionality:     # in testing mode a namefile is hard code
                            there_is_monitoring_data = application_state.update_monitoring_data()
                        else:
                            there_is_monitoring_data = True

                        if not there_is_monitoring_data:
                            print_with_time("IMPORTANT: There is NO data to MONITOR! Application name: " + application_state.application_name)
                            wait_time = AiadPredictorState.number_of_minutes_to_infer * 60
                            print_with_time("Waiting for " + str(AiadPredictorState.number_of_minutes_to_infer) + " minutes before monitoring again.")
                            #wait_time = 60                                                                                  # ONLY FOR TESTING. PAULA
                            #print_with_time("Waiting for 60 SEGUNDOS before monitoring again. ESTO SE DEBE CAMBIAR!!!!!")   # ONLY FOR TESTING. PAULA
                            if (wait_time > 0):
                                time.sleep(wait_time)
                            break
                        else:
                            print_with_time("Initiating predictions for all metrics for next_prediction_time, which is " + str(
                                application_state.next_prediction_time) + " prediction_index " + str(prediction_index))
                            
                            prediction = TestingAModel(columns_with_variability, scaler, myNSA, myKmeans, application_state, int(application_state.next_prediction_time))

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

                    any_anomaly_sent = False
                    if prediction is not None:
                        if AiadPredictorState.ai_nsa and "nsa_window_anomaly_rate" in prediction and prediction["nsa_window_anomaly_rate"] >= AiadPredictorState.ai_nsa_anomaly_rate:
                            message_not_sent = True
                            current_time = int(time.time())
                            prediction_message_body = {
                                "method": "aiad nsa",
                                "level": 3,
                                "application": application_state.application_name,
                                "node": application_state.instance,
                                "timestamp": np.int64(current_time),
                                "window_start": np.int64(prediction["nsa_data"].index.min()),
                                "window_end": np.int64(prediction["nsa_data"].index.max()),
                                "window_anomaly_rate": prediction["nsa_window_anomaly_rate"],
                                "predictionTime": np.int64(application_state.next_prediction_time),
                                #"metrics": application_state.metrics_to_predict
                                "metrics": list(columns_with_variability)
                            }
                            # Convert message to native types
                            prediction_message_body = convert_to_native(prediction_message_body)
                            while (message_not_sent):
                                try:
                                    # for publisher in State.broker_publishers:
                                    #    if publisher.
                                    for publisher in AiadPredictorState.broker_publishers:
                                        if publisher.key == "publisher_" + application_state.application_name + "-" + "allmetrics":
                                            publisher.send(prediction_message_body, application_state.application_name)
                                            print_with_time("publisher.send")

                                    any_anomaly_sent = True
                                    message_not_sent = False
                                    print_with_time(
                                        "Successfully sent anomaly detection message for %s to topic eu.nebulouscloud.preliminary_predicted.%s.%s:\n\n%s\n\n" % (
                                            "allmetrics", AiadPredictorState.forecaster_name, "allmetrics", prediction_message_body))
                                except ConnectionError as exception:
                                    logging.error("Error sending an nsa anomaly detection" + str(exception))
                                    AiadPredictorState.disconnected = False

                        if AiadPredictorState.ai_kmeans and "kmeans_window_anomaly_rate" in prediction:
                            for metric, value in prediction["kmeans_window_anomaly_rate"].items():
                                if value > AiadPredictorState.ai_kmeans_anomaly_rate:
                                    message_not_sent2 = True
                                    current_time = int(time.time())
                                    prediction_message_body = {
                                        "method": "aiad kmeans",
                                        "level": 3,
                                        "application": application_state.application_name,
                                        "node": application_state.instance,
                                        "timestamp": np.int64(current_time),
                                        "window_start": np.int64(prediction["kmeans_data"].index.min()),
                                        "window_end": np.int64(prediction["kmeans_data"].index.max()),
                                        "window_anomaly_rate": value,
                                        "predictionTime": np.int64(application_state.next_prediction_time),
                                        "metrics": metric
                                    }
                                    # Convert message to native types
                                    prediction_message_body = convert_to_native(prediction_message_body)
                                    while (message_not_sent2):
                                        try:
                                            # for publisher in State.broker_publishers:
                                            #    if publisher.
                                            for publisher in AiadPredictorState.broker_publishers:
                                                if publisher.key == "publisher_" + application_state.application_name + "-" + "allmetrics":
                                                    publisher.send(prediction_message_body, application_state.application_name)
                                                    print_with_time("publisher.send")

                                            any_anomaly_sent = True
                                            message_not_sent2 = False
                                            print_with_time(
                                                "Successfully sent anomaly detection message for %s to topic eu.nebulouscloud.preliminary_predicted.%s.%s:\n\n%s\n\n" % (
                                                    metric, AiadPredictorState.forecaster_name, "allmetrics", prediction_message_body))
                                        except ConnectionError as exception:
                                            logging.error("Error sending an kmeans anomaly detection" + str(exception))
                                            AiadPredictorState.disconnected = False
                                    
                        if not any_anomaly_sent:
                            print_with_time("NO anomaly detection message was sent for the application " + application_state.application_name + ".")
                            if "nsa_window_anomaly_rate" in prediction:
                                print_with_time("nsa window anomaly rate is "+str(prediction["nsa_window_anomaly_rate"])+" (Send when is >= " +str(AiadPredictorState.ai_nsa_anomaly_rate)+ ").")
                            if "kmeans_window_anomaly_rate" in prediction:
                                print_with_time("kmeans window anomaly rate is "+str(prediction["kmeans_window_anomaly_rate"])+" (Send when any is >= " +str(AiadPredictorState.ai_kmeans_anomaly_rate)+ ").")
                            
                        application_state.previous_prediction = prediction
                        
                    logging.info(f'Ending cycle of {application_state.application_name} (index is {prediction_index} of total_time_intervals {AiadPredictorState.total_time_intervals_to_predict} *************')
                    prediction_index += 1
        
        if there_is_modeling_data is None or there_is_monitoring_data is None:
            application_state.start_forecasting = False
            logging.info(f'Ending the anomaly detection for application {application_state.application_name} due to ERROR.')         
        else: 
            if not there_is_modeling_data:
                retries_modeling_data += 1
            
            if retries_modeling_data >= max_retries_modeling_data or not there_is_monitoring_data:
                application_state.start_forecasting = False
                logging.info(f'Ending the anomaly detection for application {application_state.application_name} due to NO data.')         
       
            if not application_state.start_forecasting:
                logging.info(f'Ending the cycle for application {application_state.application_name} --> a new monitoring data arrived at topic metric_list.')

    if not AiadPredictorState.ai_nsa and not AiadPredictorState.ai_kmeans:
        logging.info("Please, one of the variables 'ai_nsa' or 'ai_kmeans' must be set to True.")
    
    # Delete prediction_thread on completion
    if app_instance_id in prediction_thread:
        thread = prediction_thread[app_instance_id]
        thread_ident = thread.ident
        logging.info(f'Removing prediction thread for {application_state.application_name} instance {app_instance_id} --> ident: {thread_ident}.')
        del prediction_thread[app_instance_id]


def send_prediction_message(application_state, prediction, metrics, method="aiad nsa"):
    """
    Sends an anomaly message to the Artemis broker.
    Used for both NSA and KMeans.
    """
    try:
        current_time = int(time.time())
        message_body = {
            "method": method,
            "level": 3,
            "application": application_state.application_name,
            "node": application_state.instance,
            "timestamp": np.int64(current_time),
            "predictionTime": np.int64(application_state.next_prediction_time),
        }

        if method == "aiad nsa":
            message_body.update({
                "window_start": np.int64(prediction["nsa_data"].index.min()),
                "window_end": np.int64(prediction["nsa_data"].index.max()),
                "window_anomaly_rate": prediction["nsa_window_anomaly_rate"],
                "metrics": list(metrics),
            })
        elif method == "aiad kmeans":
            message_body.update({
                "window_start": np.int64(prediction["kmeans_data"].index.min()),
                "window_end": np.int64(prediction["kmeans_data"].index.max()),
                # value por cada métrica individual ya se pasa desde el bucle de KMeans
                "window_anomaly_rate": prediction["kmeans_window_anomaly_rate"].get(metrics[0], None),
                "metrics": metrics[0],
            })

        message_body = convert_to_native(message_body)

        # Attempt to send
        message_sent = False
        while not message_sent:
            try:
                for publisher in AiadPredictorState.broker_publishers:
                    if publisher.key == f"publisher_{application_state.application_name}-allmetrics":
                        publisher.send(message_body, application_state.application_name)
                        print_with_time(f"publisher.send ({method}) OK")
                message_sent = True
                print_with_time(f"Successfully sent {method.upper()} anomaly detection message for {application_state.application_name}:\n{message_body}\n")

            except ConnectionError as exception:
                logging.error(f"Error sending {method.upper()} anomaly detection: {exception}")
                AiadPredictorState.disconnected = False
                time.sleep(1)  # retry breve antes de reintentar envío

    except Exception as e:
        logging.error(f"Exception in send_prediction_message for {application_state.application_name}: {str(e)}")
        print(traceback.format_exc())


def calculate_and_publish_predictions(application_state, app_instance_id, maximum_time_required_for_prediction, prediction_thread):
    logging.info(f"We are in calculate_and_publish_predictions: application_state {application_state} app_instance_id {app_instance_id} application_state.start_forecasting {application_state.start_forecasting}")
    
    application_state.start_forecasting
    there_is_modeling_data = False
    there_is_monitoring_data = False

    while application_state.start_forecasting and (AiadPredictorState.ai_nsa or AiadPredictorState.ai_kmeans):
        print_with_time("Using " + AiadPredictorState.configuration_file_location + " for configuration details...")

        if ((application_state.previous_prediction is not None) and (
                application_state.previous_prediction["last_prediction_time_needed"] > maximum_time_required_for_prediction)):
            maximum_time_required_for_prediction = application_state.previous_prediction["last_prediction_time_needed"]

        application_state.next_prediction_time = update_prediction_time(application_state.epoch_start,
                                                                        application_state.prediction_horizon,
                                                                        maximum_time_required_for_prediction)

        # Wait before the next prediction
        wait_time = application_state.next_prediction_time - application_state.prediction_horizon - time.time()
        print_with_time("Waiting for " + str(
            round(wait_time, 2)) + " seconds, until time " + datetime.datetime.fromtimestamp(
            application_state.next_prediction_time - application_state.prediction_horizon).strftime('%Y-%m-%d %H:%M:%S'))
        if wait_time > 0:
            time.sleep(wait_time)

        Utilities.load_configuration()

        # =====================
        # MODELING DATA
        # =====================
        if not AiadPredictorState.testing_functionality:     # in testing mode a namefile is hard code
            there_is_modeling_data = application_state.update_model_data()
        else:
            there_is_modeling_data = True

        if not there_is_modeling_data:
            print_with_time(f"IMPORTANT: No TRAINING data for {application_state.application_name} instance {application_state.instance}. Exiting thread so monitor_and_rediscover can relaunch it.")
            application_state.start_forecasting = False
            break

        columns_with_variability, scaler, myNSA, myKmeans = BuildingAModel(application_state, int(application_state.next_prediction_time))
        if scaler is None and myNSA is None and myKmeans is None:
            print_with_time(f"IMPORTANT: Model could not be built for {application_state.application_name}. Exiting thread.")
            application_state.start_forecasting = False
            break

        # =====================
        # MONITORING LOOP
        # =====================
        prediction_index = 0
        while prediction_index < AiadPredictorState.total_time_intervals_to_predict and application_state.start_forecasting:
            logging.info(f'Starting cycle for {application_state.application_name} instance {application_state.instance} (index {prediction_index}/{AiadPredictorState.total_time_intervals_to_predict})')
            
            if ((application_state.previous_prediction is not None) and (
                    application_state.previous_prediction["last_prediction_time_needed"] > maximum_time_required_for_prediction)):
                maximum_time_required_for_prediction = application_state.previous_prediction["last_prediction_time_needed"]

            application_state.next_prediction_time = update_prediction_time(application_state.epoch_start,
                                                                            application_state.prediction_horizon,
                                                                            maximum_time_required_for_prediction)

            wait_time = application_state.next_prediction_time - application_state.prediction_horizon - time.time()
            print_with_time("Waiting for " + str(round(wait_time, 2)) + " seconds, until time " +
                            datetime.datetime.fromtimestamp(application_state.next_prediction_time - application_state.prediction_horizon).strftime('%Y-%m-%d %H:%M:%S'))
            if wait_time > 0:
                time.sleep(wait_time)
            
            try:
                prediction = None
                if not AiadPredictorState.testing_functionality:     # in testing mode a namefile is hard code
                    there_is_monitoring_data = application_state.update_monitoring_data()
                else:
                    there_is_monitoring_data = True

                if not there_is_monitoring_data:
                    print_with_time(f"IMPORTANT: No MONITORING data for {application_state.application_name} instance {application_state.instance}. Exiting thread so monitor_and_rediscover can relaunch it.")
                    application_state.start_forecasting = False
                    break

                print_with_time(f"Initiating predictions for next_prediction_time={application_state.next_prediction_time} instance {application_state.instance} (index {prediction_index})")
                prediction = TestingAModel(columns_with_variability, scaler, myNSA, myKmeans, application_state, int(application_state.next_prediction_time))

            except Exception:
                print_with_time(f"Could not create a prediction for {application_state.application_name}. Skipping interval {prediction_index}.")
                print(traceback.format_exc())
                break

            if (AiadPredictorState.disconnected or AiadPredictorState.check_stale_connection()):
                logging.info("Possible problem due to disconnection or stale connection")

            any_anomaly_sent = False
            if prediction is not None:
                # =====================
                # NSA anomalies
                # =====================
                logging.info(f'NSA prediction nsa_window_anomaly_rate {prediction["nsa_window_anomaly_rate"]} AiadPredictorState.ai_nsa_anomaly_rate {AiadPredictorState.ai_nsa_anomaly_rate}')
                if AiadPredictorState.ai_nsa and "nsa_window_anomaly_rate" in prediction and prediction["nsa_window_anomaly_rate"] >= AiadPredictorState.ai_nsa_anomaly_rate:
                    send_prediction_message(application_state, prediction, columns_with_variability, method="aiad nsa")
                    any_anomaly_sent = True

                # =====================
                # KMEANS anomalies
                # =====================
                if AiadPredictorState.ai_kmeans and "kmeans_window_anomaly_rate" in prediction:
                    for metric, value in prediction["kmeans_window_anomaly_rate"].items():
                        logging.info(f'kmeans metric {metric} value {value} AiadPredictorState.ai_kmeans_anomaly_rate {AiadPredictorState.ai_kmeans_anomaly_rate}')
                        if value > AiadPredictorState.ai_kmeans_anomaly_rate:
                            send_prediction_message(application_state, prediction, [metric], method="aiad kmeans")
                            any_anomaly_sent = True
                
                if not any_anomaly_sent:
                    print_with_time(f"No anomaly message sent for {application_state.application_name} instance {application_state.instance}.")
                    if "nsa_window_anomaly_rate" in prediction:
                        print_with_time(f"NSA rate: {prediction['nsa_window_anomaly_rate']} (threshold {AiadPredictorState.ai_nsa_anomaly_rate})")
                    if "kmeans_window_anomaly_rate" in prediction:
                        print_with_time(f"KMeans rates: {prediction['kmeans_window_anomaly_rate']} (threshold {AiadPredictorState.ai_kmeans_anomaly_rate})")
                
                application_state.previous_prediction = prediction

            logging.info(f'Ending cycle for {application_state.application_name} instance {application_state.instance} (index {prediction_index})')
            prediction_index += 1

    if not AiadPredictorState.ai_nsa and not AiadPredictorState.ai_kmeans:
        logging.info("Please set either 'ai_nsa' or 'ai_kmeans' to True.")

    # Final cleaning of the thread
    if app_instance_id in prediction_thread:
        thread = prediction_thread[app_instance_id]
        thread_ident = thread.ident
        logging.info(f'Removing prediction thread for {application_state.application_name} instance {app_instance_id} --> ident: {thread_ident}.')
        del prediction_thread[app_instance_id]


# class Listener(messaging.listener.MorphemicListener):
class BootStrap(ConnectorHandler):
    pass

class ConsumerHandler:
    def on_message(self, key, address, body, message: Message, context):
        address = address.replace("topic://" + AiadPredictorState.GENERAL_TOPIC_PREFIX, "")
        if address.startswith(AiadPredictorState.MONITORING_DATA_PREFIX):
            address = address.replace(AiadPredictorState.MONITORING_DATA_PREFIX, "", 1)

            if address == 'metric_list':
                application_name = body.get("name")
                message_version = body.get("version", 0)

                if not application_name or application_name is None:
                    logging.warning(f"[metric_list] Invalid or missing application name in metric_list message: {body}")
                    return                

                logging.info(f"[metric_list] Received metric_list for {application_name} v{message_version}")
                AiadPredictorState.received_applications[application_name] = message_version

                if not AiadPredictorState.metric_list_received:
                    AiadPredictorState.metric_list_received = True
                    delay = 0       # Don't wait for anything to start. PCF
                    threading.Timer(delay * 60, self.launch_application_threads).start()                    
                else:
                    logging.info(f"[metric_list] Received another metric_list for {application_name} v{message_version}... it is NOT taken into account.")

                return

    def launch_application_threads(self):
        self.manage_application_threads(initial_launch=True)

    def manage_application_threads(self, initial_launch=False):
        """
        Discovers instances and launches or relaunches the corresponding prediction threads.
        If initial_launch=True, the initial launch is considered ([launch] messages).
        """
        tag = "[launch]" if initial_launch else "[monitor]"
        logging.info(f"{tag} {'Launching' if initial_launch else 'Checking'} application threads...")

        for app_name, version in AiadPredictorState.received_applications.items():
            bucket = AiadPredictorState.application_name_prefix + app_name + "_bucket"
            discovery = InstanceDiscovery(
                influxdb_bucket=bucket,
                influxdb_organization=AiadPredictorState.influxdb_organization
            )
            instances = discovery.get_all_instances(AiadPredictorState.number_of_days_to_use_data_from)

            for inst_id in instances:
                app_instance_id = f"{app_name}@{inst_id}"

                thread_missing = (
                    app_instance_id not in AiadPredictorState.prediction_thread or
                    not AiadPredictorState.prediction_thread[app_instance_id].is_alive()
                )

                if thread_missing:
                    msg = "Starting" if initial_launch else "Relaunching missing/dead"
                    logging.info(f"{tag} {msg} prediction thread for {app_instance_id}")

                    app_state = ApplicationState(app_name, version, inst_id)
                    AiadPredictorState.applications_state[app_instance_id] = app_state

                    thread = threading.Thread(
                        target=calculate_and_publish_predictions,
                        args=(
                            app_state,
                            app_instance_id,
                            AiadPredictorState.prediction_processing_time_safety_margin_seconds,
                            AiadPredictorState.prediction_thread
                        ),
                        daemon=True
                    )
                    thread.start()
                    AiadPredictorState.prediction_thread[app_instance_id] = thread

                    # To avoid saturation 120 seconds
                    time.sleep(120)

        # Reschedule the next periodic check
        interval = AiadPredictorState.number_of_minutes_to_detect_instances_or_check_everything_ok
        threading.Timer(interval * 60, lambda: self.manage_application_threads(False)).start()


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
