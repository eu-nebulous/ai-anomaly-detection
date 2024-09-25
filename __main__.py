# ### AI anomaly detection - NebulOuS
#
# Example of use... Console 1 & 2
# ***** Console #1
# kubectl -n nebulous-cd get pods					(-n means... Specify a namespace with the -n or --namespace flag)
# .... get nebulous-activemq-xxxxx pod name		    (for example nebulous-activemq-875988f58-gf45f)
# kubectl -n nebulous-cd port-forward nebulous-activemq-xxxxxx 32754:32754
#												    (for example kubectl -n nebulous-cd port-forward nebulous-activemq-875988f58-gf45f 5672:5672)
# kubectl -n nebulous-cd port-forward nebulous-activemq-6b7f4cd65-p58xs 32754:32754
#
# ***** Console #2
# python __main__.py configs/config_local.yml
# python __main__.py configs/config.yml

import logging

# Set logging format
LOGFORMAT = "%(asctime)s,%(name)s [%(levelname)s] %(message)s"
LOGDATEFORMAT = "%Y-%m-%dT%H:%M:%S"
LOGFILENAME = "anomalydetection.log"
logging.basicConfig(filename=LOGFILENAME, filemode='w', level=logging.INFO, format=LOGFORMAT, datefmt=LOGDATEFORMAT)
logger = logging.getLogger(__name__)

import sys
import os
import sys
import threading
import time
import calendar
from dotenv import load_dotenv
from exn.core.publisher import Publisher
from exn.handler.connector_handler import ConnectorHandler
from exn import connector, core
import numpy as np
import pandas as pd

# Define constants and configuration variables
COMPONENT_NAME = 'anomaly_detector'
TOPIC_NAME = 'anomaly_detected'
APPLICATION_ID = os.environ.get('APPLICATION_ID', '')
BROKER_ADDRESS = os.environ.get('BROKER_ADDRESS', '158.37.63.86')
BROKER_PORT = int(os.environ.get('BROKER_PORT', 32754))
BROKER_USERNAME = os.environ.get('BROKER_USERNAME', 'admin')
BROKER_PASSWORD = os.environ.get('BROKER_PASSWORD', 'admin')
RETRY_CONNECT = os.environ.get('RETRY_CONNECT', 5)

from utils.config import load_config_from_yaml, Config as ConfigClass
from src.ai_anomaly_detection_nebulous import Main_Loop

class Bootstrap(ConnectorHandler):

    def ready(self, context):
        if context.has_publisher('state'):
            context.publishers['state'].starting()
            context.publishers['state'].started()
            context.publishers['state'].custom('working_hard')
            """context.publishers['state'].stopping()
            context.publishers['state'].stopped()"""

        """context.publishers['config'].send({
            'hello': 'world'
        },application="one")

        context.publishers['config'].send({
            'good': 'bye'
        },application="two")"""

        """if context.has_publisher(TOPIC_NAME):
            context.publishers[TOPIC_NAME].send()"""
            
        Config: ConfigClass = load_config_from_yaml(sys.argv[1])
        
        ai_last_n_hours = Config.AI_LAST_N_HOURS
        ai_last_n_hours_test = Config.AI_LAST_N_HOURS_TEST
        ai_last_n_minutes_test = Config.AI_LAST_N_MINUTES_TEST
        ai_train = Config.AI_TRAIN
        ai_kmeans = Config.AI_KMEANS
        ai_nsa = Config.AI_NSA
        ai_graph = Config.AI_GRAPH
        ai_ip = Config.AI_IP
        ai_port = Config.AI_PORT
        ai_charts = Config.AI_CHARTS
        ai_dims = Config.AI_DIMS
        ai_interval = Config.AI_INTERVAL
        ai_iterations = Config.AI_ITERATIONS
        ai_nsa_num_samples_to_lag = Config.AI_NSA_NUM_SAMPLES_TO_LAG
        ai_nsa_num_samples_to_diff = Config.AI_NSA_NUM_SAMPLES_TO_DIFF
        ai_nsa_num_samples_to_smooth = Config.AI_NSA_NUM_SAMPLES_TO_SMOOTH
        ai_nsa_max_t_cells = Config.AI_NSA_MAX_T_CELLS
        ai_nsa_max_attempts = Config.AI_NSA_MAX_ATTEMPTS
        ai_nsa_percentage = Config.AI_NSA_PERCENTAGE
        ai_nsa_algorithm = Config.AI_NSA_ALGORITHM
        ai_nsa_n_neighbors = Config.AI_NSA_N_NEIGHBORS
        ai_nsa_anomaly_rate = Config.AI_NSA_ANOMALY_RATE
        ai_nsa_kmeans_anomaly_rate = Config.AI_NSA_KMEANS_ANOMALY_RATE
        ai_kmeans_num_samples_to_lag = Config.AI_KMEANS_NUM_SAMPLES_TO_LAG
        ai_kmeans_num_samples_to_diff = Config.AI_KMEANS_NUM_SAMPLES_TO_DIFF
        ai_kmeans_num_samples_to_smooth = Config.AI_KMEANS_NUM_SAMPLES_TO_SMOOTH
        ai_kmeans_anomaly_rate = Config.AI_KMEANS_ANOMALY_RATE
        ai_kmeans_max_iterations = Config.AI_KMEANS_MAX_ITERATIONS            
            
        
        t1 = threading.Thread(target=Main_Loop, args=(context, TOPIC_NAME,
        ai_last_n_hours, ai_last_n_hours_test, ai_last_n_minutes_test, ai_train, ai_kmeans, ai_nsa, 
        ai_graph, ai_ip, ai_port, ai_charts, ai_dims, ai_interval, ai_iterations,
        ai_nsa_num_samples_to_lag, ai_nsa_num_samples_to_diff, ai_nsa_num_samples_to_smooth, 
        ai_nsa_max_t_cells, ai_nsa_max_attempts, ai_nsa_percentage, ai_nsa_algorithm, ai_nsa_n_neighbors,
        ai_kmeans_num_samples_to_lag, ai_kmeans_num_samples_to_diff, ai_kmeans_num_samples_to_smooth,
        ai_nsa_anomaly_rate, ai_kmeans_anomaly_rate, ai_nsa_kmeans_anomaly_rate, ai_kmeans_max_iterations))
        
        t1.start()


# Convert numpy.float64 and numpy.int64 to float and int in dictionary
def convert_numpy_float64_int_64_df(d):
    for k, v in d.items():
        if isinstance(v, np.float64):
            d[k] = float(v)
        elif isinstance(v, np.int64):
            d[k] = int(v)            
        elif isinstance(v, pd.DataFrame):
            d[k] = v.to_dict(orient='list')  # Convert DataFrame to dictionary
        elif isinstance(v, dict):
            convert_numpy_float64_int_64_df(v)
    return d


class MyPublisher(Publisher):  

    def __init__(self):
        super().__init__( TOPIC_NAME, TOPIC_NAME, topic=True)

    def send(self, anomaly_data):
        anomaly_data = convert_numpy_float64_int_64_df(anomaly_data)
        super(MyPublisher, self).send(anomaly_data,application=APPLICATION_ID)


if __name__ == '__main__':
    
    # Initialize the connector
    connector = connector.EXN(
        COMPONENT_NAME,
        handler=Bootstrap(),
        publishers=[MyPublisher()],
        enable_health=True,
        enable_state=True,
        url=BROKER_ADDRESS,
        port=BROKER_PORT,
        username=BROKER_USERNAME,
        password=BROKER_PASSWORD
    )
    
    print('Starting EXN connector...')
    logger.info('Starting EXN connector...')
    connector.start()
