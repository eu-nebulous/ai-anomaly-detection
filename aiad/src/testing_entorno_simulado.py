# Las aplicaciones
# - influxdb_bucket = "nebulous_ApplicationPaula_bucket"
# - influxdb_bucket = "nebulous_Application2Paula_bucket"
# deben crearse a través de http://localhost:8086/signin    login: my-user  password: my-password
# 
#
# IMPORTANT: Please, change what you want before running!!!
# Ver 'ANALIZAR PAULA' y modificar según lo que se desea realizar
# python generating_influxdb_data.py
#
# para probar el entorno 
# python testing_entorno_simulado.py
#
import sys
from unittest.mock import MagicMock
import time

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

# *********************************************************************************************
key = "example_key"
address = "topic://eu.nebulouscloud.monitoring.metric_list"
body = {
    "name": "ApplicationPaula",
    "version": 1,
    "metric_list": [
        {"name": "system_cpu_user", "lower_bound": 0.0, "upper_bound": 100.0},
        {"name": "system_ram_used", "lower_bound": 0.0, "upper_bound": 10000.0},
        {"name": "system_cpu_system", "lower_bound": 0.0, "upper_bound": 100.0},
    ],
}
kwargs = {}

# Enviar el mensaje al método
consumer_handler.on_message(key, address, body, mock_context, **kwargs)
print('A message metric_list has been sent.... #1 #####################################################')

# *********************************************************************************************

waiting_time = 20
print('Waiting ' + str(waiting_time) + ' seconds to send a metric_list message')
time.sleep(waiting_time) # wait X seconds

# *********************************************************************************************
key = "example_key"
address = "topic://eu.nebulouscloud.monitoring.metric_list"
body = {
    "name": "Application2Paula",
    "version": 1,
    "metric_list": [
        {"name": "system_cpu_user", "lower_bound": 0.0, "upper_bound": 100.0},
        {"name": "system_ram_used", "lower_bound": 0.0, "upper_bound": 10000.0},
        {"name": "system_cpu_system", "lower_bound": 0.0, "upper_bound": 100.0},
    ],
}
kwargs = {}

# Enviar el mensaje al método
consumer_handler.on_message(key, address, body, mock_context, **kwargs)
print('A message metric_list has been sent.... #2 #####################################################')

# *********************************************************************************************

waiting_time = 500
print('Waiting ' + str(waiting_time) + ' seconds to send a metric_list message')
time.sleep(waiting_time) # wait X seconds... more than one interval (now 500 seconds)

# *********************************************************************************************

key = "example_key"
address = "topic://eu.nebulouscloud.monitoring.metric_list"
body = {
    "name": "ApplicationPaula",
    "version": 1,
    "metric_list": [
        {"name": "system_cpu_user", "lower_bound": 0.0, "upper_bound": 100.0},
        {"name": "system_ram_used", "lower_bound": 0.0, "upper_bound": 8000.0},
        {"name": "system_cpu_system", "lower_bound": 0.0, "upper_bound": 100.0},
    ],
}
kwargs = {}

# Enviar el mensaje al método
consumer_handler.on_message(key, address, body, mock_context, **kwargs)
print('A message metric_list has been sent.... #3 #####################################################')

# *********************************************************************************************

waiting_time = 500
print('Waiting ' + str(waiting_time) + ' seconds to send a metric_list message')
time.sleep(waiting_time) # wait X seconds... more than one interval (now 500 seconds)

# *********************************************************************************************

key = "example_key"
address = "topic://eu.nebulouscloud.monitoring.metric_list"
body = {
    "name": "ApplicationPaula",
    "version": 1,
    "metric_list": [
        {"name": "system_cpu_user", "lower_bound": 0.0, "upper_bound": 50.0},
        {"name": "system_ram_used", "lower_bound": 0.0, "upper_bound": 6000.0},
        {"name": "system_cpu_system", "lower_bound": 0.0, "upper_bound": 50.0},
    ],
}
kwargs = {}

# Enviar el mensaje al método
consumer_handler.on_message(key, address, body, mock_context, **kwargs)
print('A message metric_list has been sent.... #4 #####################################################')

# *********************************************************************************************

waiting_time = 500
print('Waiting ' + str(waiting_time) + ' seconds to send a metric_list message')
time.sleep(waiting_time) # wait X seconds... more than one interval (now 500 seconds)

# *********************************************************************************************

key = "example_key"
address = "topic://eu.nebulouscloud.monitoring.metric_list"
body = {
    "name": "ApplicationPaula",
    "version": 1,
    "metric_list": [
        {"name": "system_cpu_user", "lower_bound": 0.0, "upper_bound": 100.0},
        {"name": "system_ram_used", "lower_bound": 0.0, "upper_bound": 10000.0},
        {"name": "system_cpu_system", "lower_bound": 0.0, "upper_bound": 100.0},
    ],
}
kwargs = {}

# Enviar el mensaje al método
consumer_handler.on_message(key, address, body, mock_context, **kwargs)
print('A message metric_list has been sent.... #4 #####################################################')
