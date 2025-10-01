# para probar el entorno 
# python testing_entorno_simulado.py
#
import sys
from unittest.mock import MagicMock
import time
from proton import Message

# Asegúrate de que 'runtime' esté en el sys.path
sys.path.append('./runtime')

# Importar la clase ConsumerHandler desde Predictor.py
from Predictor import ConsumerHandler

# Instanciar la clase
consumer_handler = ConsumerHandler()

# Crear un contexto simulado
mock_context = MagicMock()
mock_context.has_publisher.return_value = True
mock_context.publishers = {'state': MagicMock()}
mock_message = Message()


# *********************************************************************************************
key = "example_key"
address = "topic://eu.nebulouscloud.monitoring.metric_list"
body = {
    #"name": "7d634646-6598-48b5-ab75-9b864bea12d9",
    "name": "133c50df-c7c2-4c2b-83e3-689cae735607",
    "version": 1,
    "metric_list": [
        {"name": "system_cpu_user", "lower_bound": "-Infinity", "upper_bound": "Infinity"},
        {"name": "system_ram_used", "lower_bound": "-Infinity", "upper_bound": "Infinity"},
        {"name": "system_cpu_system", "lower_bound": "-Infinity", "upper_bound": "Infinity"},
    ],
}
kwargs = {}

# Enviar el mensaje al método
consumer_handler.on_message(key, address, body, mock_message, mock_context)
print('A message metric_list has been sent.... #0 #####################################################')

waiting_time = 180
print('Waiting ' + str(waiting_time) + ' seconds to send another metric_list message. Paula')
time.sleep(waiting_time) # wait X seconds


key = "example_key"
address = "topic://eu.nebulouscloud.monitoring.metric_list"
body = {
    #"name": "7d634646-6598-48b5-ab75-9b864bea12d9",
    "name": "133c50df-c7c2-4c2b-83e3-689cae735607",
    "version": 1,
    "metric_list": [
        {"name": "system_cpu_user", "lower_bound": "-Infinity", "upper_bound": "Infinity"},
        {"name": "system_ram_used", "lower_bound": "-Infinity", "upper_bound": "Infinity"},
        {"name": "system_cpu_system", "lower_bound": "-Infinity", "upper_bound": "Infinity"},
    ],
}
kwargs = {}

# Enviar el mensaje al método
consumer_handler.on_message(key, address, body, mock_message, mock_context)
print('A message metric_list has been sent.... #0 #####################################################')


