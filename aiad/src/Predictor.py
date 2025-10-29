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
from influxdb_client import InfluxDBClient, Point, WritePrecision
from runtime.utilities.InfluxDBConnector import InfluxDBConnector

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


def write_prediction_to_influx(application_state, message_body, should_send):
    """
    Save the anomaly prediction message (NSA or K-Means) to the corresponding application bucket.
    """
    influx = None
    try:
        influx = InfluxDBConnector()

        metrics_value = message_body["metrics"]
        if isinstance(metrics_value, list):
            metrics_value = ",".join(metrics_value)

        if message_body["method"] == "aiad nsa":
            measurement_name = f"_predicted_{message_body['method'].replace(' ', '_')}_{message_body['node']}"
        elif message_body["method"] == "aiad kmeans":
            measurement_name = f"_predicted_{message_body['method'].replace(' ', '_')}_{message_body['node']}_{metrics_value}"

        point = (
            Point(measurement_name)
            .tag("application_name", message_body["application"])
            .tag("node", message_body["node"])
            .tag("method", message_body["method"])
            .tag("metrics", metrics_value)
            .tag("level", str(message_body["level"]))
            .field("metricValue", float(message_body["window_anomaly_rate"]))
            .field("anomaly", should_send)
            .field("predictionTime", int(message_body["predictionTime"]))
            .field("window_start", int(message_body["window_start"]))
            .field("window_end", int(message_body["window_end"]))
        )
        point.time(int(message_body["timestamp"]), write_precision=WritePrecision.S)

        influx.write_data(point, application_state.influxdb_bucket)
        logging.info(
            f"[InfluxDB] Saved anomaly | app={message_body['application']} | node={message_body['node']} | "
            f"method={message_body['method']} | rate={message_body['window_anomaly_rate']:.2f} | "
            f"metrics={message_body['metrics']}"
        )

    except Exception as e:
        logging.error(f"Error writing prediction to InfluxDB: {e}")
    finally:
        if influx:
            try:
                influx.close()
            except Exception:
                pass


def send_prediction_message(application_state, prediction, metrics, method="aiad nsa", force_influx=False):
    """
    Sends an anomaly message to the Artemis broker and always stores it in InfluxDB.
    If force_influx=True, the message is written to InfluxDB even if not sent to the broker.
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
                "window_anomaly_rate": prediction["kmeans_window_anomaly_rate"].get(metrics[0], None),
                "metrics": metrics[0],
            })

        message_body = convert_to_native(message_body)

        should_send = False
        if method == "aiad nsa":
            should_send = message_body["window_anomaly_rate"] >= AiadPredictorState.ai_nsa_anomaly_rate
        elif method == "aiad kmeans":
            should_send = message_body["window_anomaly_rate"] >= AiadPredictorState.ai_kmeans_anomaly_rate

        if should_send:
            max_retries = 5
            retry_count = 0
            message_sent = False

            while not message_sent and retry_count < max_retries:
                try:
                    for publisher in AiadPredictorState.broker_publishers:
                        if publisher.key == f"publisher_{application_state.application_name}-allmetrics":
                            publisher.send(message_body, application_state.application_name)
                            print_with_time(f"publisher.send ({method}) OK")

                    message_sent = True
                    print_with_time(f"Successfully sent {method.upper()} anomaly detection message for {application_state.application_name}:\n{message_body}\n")

                except ConnectionError as exception:
                    retry_count += 1
                    logging.error(f"Error sending {method.upper()} anomaly detection (attempt {retry_count}/{max_retries}): {exception}")
                    AiadPredictorState.disconnected = False
                    time.sleep(2)  # wait before retrying

            if not message_sent:
                logging.error(f"Failed to send {method.upper()} message after {max_retries} attempts for {application_state.application_name}. Skipping.")
        else:
            logging.info(f"[Broker skipped] {method.upper()} anomaly rate below threshold.")
            
        # Save in InfluxDB
        write_prediction_to_influx(application_state, message_body, should_send)

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
                #if AiadPredictorState.ai_nsa and "nsa_window_anomaly_rate" in prediction and prediction["nsa_window_anomaly_rate"] >= AiadPredictorState.ai_nsa_anomaly_rate:
                if AiadPredictorState.ai_nsa and "nsa_window_anomaly_rate" in prediction:
                    send_prediction_message(application_state, prediction, columns_with_variability, method="aiad nsa")
                    if prediction["nsa_window_anomaly_rate"] >= AiadPredictorState.ai_nsa_anomaly_rate:
                        any_anomaly_sent = True

                # =====================
                # KMEANS anomalies
                # =====================
                if AiadPredictorState.ai_kmeans and "kmeans_window_anomaly_rate" in prediction:
                    for metric, value in prediction["kmeans_window_anomaly_rate"].items():
                        logging.info(f'kmeans metric {metric} value {value} AiadPredictorState.ai_kmeans_anomaly_rate {AiadPredictorState.ai_kmeans_anomaly_rate}')
                        send_prediction_message(application_state, prediction, [metric], method="aiad kmeans")
                        if value > AiadPredictorState.ai_kmeans_anomaly_rate:
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
