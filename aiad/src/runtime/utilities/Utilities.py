import datetime
import logging,os
import json
from influxdb_client import InfluxDBClient

from runtime.operational_status.AiadPredictorState import AiadPredictorState


class Utilities:

    @staticmethod
    def print_with_time(x):
        now = datetime.datetime.now()
        print("[" + now.strftime('%Y-%m-%d %H:%M:%S') + "] " + str(x))

    @staticmethod
    def get_config_value(env_var, config_key):
        """
        Returns the value of an environment variable if it is set;
        otherwise returns the value from the configuration file.
        """
        env_val = os.getenv(env_var)
        if env_val is not None:
            logging.info(f"Overriding {config_key} with environment variable {env_var}: {env_val}")
            return env_val
        else:
            return AiadPredictorState.configuration_details.get(config_key).data

    @staticmethod
    def load_configuration():
        # First load configuration details from the properties file.
        with open(AiadPredictorState.configuration_file_location, 'rb') as config_file:
            AiadPredictorState.configuration_details.load(config_file)

        # Now, override each value with an environment variable if available.
        AiadPredictorState.number_of_days_to_use_data_from = int(
            Utilities.get_config_value("NUMBER_OF_DAYS_TO_USE_DATA_FROM", "number_of_days_to_use_data_from")
        )

        AiadPredictorState.number_of_minutes_to_infer = int(
            Utilities.get_config_value("NUMBER_OF_MINUTES_TO_INFER", "number_of_minutes_to_infer")
        )

        AiadPredictorState.prediction_processing_time_safety_margin_seconds = int(
            Utilities.get_config_value("PREDICTION_PROCESSING_TIME_SAFETY_MARGIN_SECONDS", "prediction_processing_time_safety_margin_seconds")
        )

        AiadPredictorState.testing_functionality = Utilities.get_config_value(
            "TESTING_FUNCTIONALITY", "testing_functionality"
        ).lower() == "true"

        AiadPredictorState.path_to_datasets = Utilities.get_config_value(
            "PATH_TO_DATASETS", "path_to_datasets"
        )

        AiadPredictorState.broker_address = Utilities.get_config_value(
            "BROKER_ADDRESS", "broker_address"
        )

        AiadPredictorState.broker_port = int(
            Utilities.get_config_value("BROKER_PORT", "broker_port")
        )

        AiadPredictorState.broker_username = Utilities.get_config_value(
            "BROKER_USERNAME", "broker_username"
        )

        AiadPredictorState.broker_password = Utilities.get_config_value(
            "BROKER_PASSWORD", "broker_password"
        )

        AiadPredictorState.ai_nsa = Utilities.get_config_value(
            "AI_NSA", "ai_nsa"
        ).lower() == "true"

        AiadPredictorState.ai_nsa_anomaly_rate = float(
            Utilities.get_config_value("AI_NSA_ANOMALY_RATE", "ai_nsa_anomaly_rate")
        )

        AiadPredictorState.ai_kmeans = Utilities.get_config_value(
            "AI_KMEANS", "ai_kmeans"
        ).lower() == "true"

        AiadPredictorState.ai_kmeans_anomaly_rate = float(
            Utilities.get_config_value("AI_KMEANS_ANOMALY_RATE", "ai_kmeans_anomaly_rate")
        )

        AiadPredictorState.influxdb_hostname = Utilities.get_config_value(
            "INFLUXDB_HOSTNAME", "INFLUXDB_HOSTNAME"
        )

        AiadPredictorState.influxdb_port = int(
            Utilities.get_config_value("INFLUXDB_PORT", "INFLUXDB_PORT")
        )

        AiadPredictorState.influxdb_username = Utilities.get_config_value(
            "INFLUXDB_USERNAME", "INFLUXDB_USERNAME"
        )

        AiadPredictorState.influxdb_token = Utilities.get_config_value(
            "INFLUXDB_TOKEN", "INFLUXDB_TOKEN"
        )

        AiadPredictorState.influxdb_org = Utilities.get_config_value(
            "INFLUXDB_ORG", "INFLUXDB_ORG"
        )

        #Log the effective configuration.
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

