import threading, logging

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
    influxdb_username = "my-user"
    influxdb_port = 8086
    influxdb_hostname = "localhost"            # PCF-LOCAL
    #influxdb_hostname = "nebulous-influxdb"
    path_to_datasets = "./datasets/"
    number_of_days_to_use_data_from = 1
    number_of_minutes_to_infer = 120    # 2 hours
    number_of_minutes_to_detect_instances_or_check_everything_ok = 10
    min_number_of_records_to_model = 50

    ai_nsa = True
    ai_nsa_anomaly_rate = 20            # Means percentage (20%)
    ai_kmeans = True
    ai_kmeans_anomaly_rate = 20         # Means percentage (20%)

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
    broker_address = "158.37.63.86"       #PCF-LOCAL
    #broker_address = "nebulous-activemq"
    # PCF USO OTRO PUERTO PARA PROBAR broker_port = 5672
    broker_port = 32754                    #PCF-LOCAL
    #broker_port = 5672
    broker_username = "admin"
    #broker_password = "admin"
    broker_password = "nebulous"
    
    prediction_thread = {}
    applications_state = {}
    received_applications = {}
    metric_list_received = False


    @staticmethod
    #TODO inspect State.connection
    def check_stale_connection():
        return (not AiadPredictorState.subscribing_connector)


