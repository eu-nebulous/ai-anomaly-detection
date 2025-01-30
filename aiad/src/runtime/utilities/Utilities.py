# Copyright (c) 2023 Institute of Communication and Computer Systems
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.        

#from morphemic.dataset import DatasetMaker
import datetime
import logging,os
import json
from influxdb_client import InfluxDBClient

from runtime.operational_status.AiadPredictorState import AiadPredictorState


class Utilities:

    @staticmethod
    def print_with_time(x):
        now = datetime.datetime.now()
        print("["+now.strftime('%Y-%m-%d %H:%M:%S')+"] "+str(x))

    @staticmethod
    def load_configuration():
        with open(AiadPredictorState.configuration_file_location, 'rb') as config_file:
            AiadPredictorState.configuration_details.load(config_file)
            AiadPredictorState.number_of_days_to_use_data_from = int(AiadPredictorState.configuration_details.get("number_of_days_to_use_data_from").data)
            AiadPredictorState.number_of_minutes_to_infer = int(AiadPredictorState.configuration_details.get("number_of_minutes_to_infer").data)
            AiadPredictorState.prediction_processing_time_safety_margin_seconds = int(AiadPredictorState.configuration_details.get("prediction_processing_time_safety_margin_seconds").data)
            AiadPredictorState.testing_functionality = AiadPredictorState.configuration_details.get("testing_functionality").data.lower() == "true"
            AiadPredictorState.path_to_datasets = AiadPredictorState.configuration_details.get("path_to_datasets").data
            AiadPredictorState.broker_address = AiadPredictorState.configuration_details.get("broker_address").data
            AiadPredictorState.broker_port = int(AiadPredictorState.configuration_details.get("broker_port").data)
            AiadPredictorState.broker_username = AiadPredictorState.configuration_details.get("broker_username").data
            AiadPredictorState.broker_password = AiadPredictorState.configuration_details.get("broker_password").data

            AiadPredictorState.ai_nsa = AiadPredictorState.configuration_details.get("ai_nsa").data.lower() == "true"
            AiadPredictorState.ai_nsa_anomaly_rate = float(AiadPredictorState.configuration_details.get("ai_nsa_anomaly_rate").data)
            AiadPredictorState.ai_kmeans = AiadPredictorState.configuration_details.get("ai_kmeans").data.lower() == "true"
            AiadPredictorState.ai_kmeans_anomaly_rate = float(AiadPredictorState.configuration_details.get("ai_kmeans_anomaly_rate").data)

            AiadPredictorState.influxdb_hostname = AiadPredictorState.configuration_details.get("INFLUXDB_HOSTNAME").data
            AiadPredictorState.influxdb_port = int(AiadPredictorState.configuration_details.get("INFLUXDB_PORT").data)
            AiadPredictorState.influxdb_username = AiadPredictorState.configuration_details.get("INFLUXDB_USERNAME").data
            AiadPredictorState.influxdb_password = AiadPredictorState.configuration_details.get("INFLUXDB_PASSWORD").data
            AiadPredictorState.influxdb_org = AiadPredictorState.configuration_details.get("INFLUXDB_ORG").data

        #This method accesses influx db to retrieve the most recent metric values.
            Utilities.print_with_time("The configuration effective currently is the following\n "+Utilities.get_fields_and_values(AiadPredictorState))

    @staticmethod
    def update_influxdb_organization_id():
        logging.info("InfluxDB http://" + AiadPredictorState.influxdb_hostname + ":" + str(AiadPredictorState.influxdb_port))
        client = InfluxDBClient(url="http://" + AiadPredictorState.influxdb_hostname + ":" + str(AiadPredictorState.influxdb_port), token=AiadPredictorState.influxdb_token)
        org_api = client.organizations_api()
        # List all organizations
        organizations = org_api.find_organizations()

        # Find the organization by name and print its ID
        for org in organizations:
            if org.name == AiadPredictorState.influxdb_organization:
                logging.info(f"Organization Name: {org.name}, ID: {org.id}")
                AiadPredictorState.influxdb_organization_id = org.id
                break
    @staticmethod
    def fix_path_ending(path):
        if (path[-1] is os.sep):
            return path
        else:
            return path + os.sep

    @staticmethod
    def default_to_string(obj):
        return str(obj)
    @classmethod
    def get_fields_and_values(cls,object):
        #Returns those fields that do not start with __ (and their values)
        fields_values = {key: value for key, value in object.__dict__.items() if not key.startswith("__")}
        return json.dumps(fields_values,indent=4,default=cls.default_to_string)

