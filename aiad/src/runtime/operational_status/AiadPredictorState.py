# Copyright (c) 2023 Institute of Communication and Computer Systems
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.        

import threading, logging

#from influxdb_client import InfluxDBClient
from jproperties import Properties

class AiadPredictorState:

    """
    The name of the aiad
    """
    forecaster_name = "aiad"
    """
    A dictionary containing statistics on the application state of individual applications
    """
    individual_application_state = {}
    """
    Fail-safe default values introduced below
    """
    application_name_prefix = "nebulous_"
    GENERAL_TOPIC_PREFIX = "eu.nebulouscloud."
    MONITORING_DATA_PREFIX = "monitoring."

    #Used to create the dataset from the InfluxDB
    influxdb_organization = "my-org"
    influxdb_organization_id = "e0033247dcca0c54"
    influxdb_token = "my-super-secret-auth-token"
    influxdb_password = "my-password"
    influxdb_username = "my-user"
    influxdb_port = 8086
    #influxdb_hostname = "localhost"            PCF-LOCAL
    influxdb_hostname = "nebulous-influxdb"
    path_to_datasets = "./datasets/"
    number_of_days_to_use_data_from = 2
    number_of_minutes_to_infer = 180

    ai_nsa = True
    ai_nsa_anomaly_rate = 10            # Means percentage (10%)
    ai_kmeans = True
    ai_kmeans_anomaly_rate = 10         # Means percentage (10%)

    configuration_file_location="aiad/prediction_configuration.properties"
    configuration_details = Properties()
    prediction_processing_time_safety_margin_seconds = 100
    disconnected = True
    disconnection_handler = threading.Condition()
    testing_functionality = False
    #testing_functionality = True
    total_time_intervals_to_predict = 12

    #Connection details
    subscribing_connector = None
    publishing_connector = None
    broker_publishers = []
    broker_consumers = []
    connector = None
    # PCF USO OTRA ADDRESS broker_address = "localhost"
    # broker_address = "158.37.63.86"
    broker_address = "nebulous-activemq"
    # PCF USO OTRO PUERTO PARA PROBAR broker_port = 5672
    #broker_port = 32754                    PCF-LOCAL
    broker_port = 5672
    broker_username = "admin"
    broker_password = "admin"


    @staticmethod
    #TODO inspect State.connection
    def check_stale_connection():
        return (not AiadPredictorState.subscribing_connector)


