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
import logging
import time
from datetime import datetime, timedelta
from influxdb_client import InfluxDBClient, Point, WritePrecision
import random

class InfluxDBQueryExporter:
    def __init__(self, influxdb_url, influxdb_token, influxdb_org, influxdb_bucket):
        self.influxdb_url = influxdb_url
        self.influxdb_token = influxdb_token
        self.influxdb_org = influxdb_org
        self.influxdb_bucket = influxdb_bucket
        self.client = InfluxDBClient(url=self.influxdb_url, token=self.influxdb_token, org=self.influxdb_org)

    def get_model_data_filename(self, configuration_file_location, metric_name):
        # Genera un nombre de archivo basado en la métrica
        return f"{configuration_file_location}/{metric_name}_dataset.csv"

    def query_and_save_metric(self, metric_name, start_time_iso, end_time_iso, configuration_file_location, print_data_from_db=False):
        # Construir la consulta Flux
        query_string = (
            f'from(bucket: "{self.influxdb_bucket}") '
            f'|> range(start: {start_time_iso}, stop: {end_time_iso}) '
            f'|> filter(fn: (r) => r["_measurement"] == "{metric_name}") '
            f'|> filter(fn: (r) => r["_field"] == "value")'
        )
        #    f'|> filter(fn: (r) => r["_measurement"] == "system_metrics") '
        #    f'|> filter(fn: (r) => r["metric"] == "{metric_name}") '

        try:
            # Ejecutar la consulta
            current_time = time.time()
            print(query_string)
            print(self.influxdb_org)
            result = self.client.query_api().query(query_string, self.influxdb_org)
            elapsed_time = time.time() - current_time

    
            if result and len(result) > 0:
                # Generar el nombre del archivo
                model_dataset_filename = self.get_model_data_filename(configuration_file_location, metric_name)

                logging.info(
                    f"Performed query to the database, it took {elapsed_time:.2f} seconds "
                    f"to receive {len(result[0].records)} entries."
                )

                # Escribir los resultados en el archivo CSV
                with open(model_dataset_filename, 'w') as file:
                    file.write("Timestamp,ems_time," + metric_name + "\r\n")  # Cabecera
                    for table in result:
                        for record in table.records:
                            record_time = record.get_time()
                            if isinstance(record_time, str):  # Si es un string
                                dt = parser.isoparse(record_time)
                            elif isinstance(record_time, datetime):  # Si ya es un objeto datetime
                                dt = record_time
                            epoch_time = int(dt.timestamp())
                            metric_value = record.get_value()
                            file.write(f"{epoch_time},{epoch_time},{metric_value}\r\n")  # Escribir datos
                            if print_data_from_db:
                                print(f"{epoch_time},{epoch_time},{metric_value}")
            else:
                logging.warning("Query returned no results.")
        except Exception as e:
            logging.error(f"Error querying InfluxDB: {e}")
        finally:
            #self.client.close()
            print('close')

def delete_data(delete_api, metric_name):
    # Eliminar datos anteriores
    start_time = "1970-01-01T00:00:00Z"  # Timestamp inicial
    end_time = datetime.utcnow().isoformat() + "Z"  # Fecha y hora actuales en formato RFC3339Nano

    # Borra todo
    try:
        # Intentar eliminar registros
        delete_api.delete(
            start=start_time,
            stop=end_time,
            predicate='',
            bucket=influxdb_bucket,
            org=influxdb_org
        )
        print("Datos eliminados correctamente.")
    except Exception as e:
        print(f"Error eliminando datos: {e}")  
        
    # One métrics
    # try:
        # # Asegúrate de pasar el predicado como una cadena
        # delete_api.delete(
            # start=start_time,
            # stop=end_time,
            # predicate=f'_measurement="{metric_name}"',
            # bucket=influxdb_bucket,  # Asegúrate de que este valor esté correctamente definido
            # org=influxdb_org  # Asegúrate de que este valor esté correctamente definido
        # )
        # print("Datos eliminados correctamente.")
    # except Exception as e:
        # print(f"Error eliminando datos: {e}")
        
        
# Generar datos para cada segundo de los últimos 4 días
def generate_dataBack2(start_time, end_time):

    # Genera datos para cada segundo dentro de ese rango
    data_points = []
    current_time = start_time
    while current_time <= end_time:
        ems_time = int(time.time())  # Tiempo en segundos
        ram_usage = random.randint(1000, 8000)  # RAM usada en MB  

        point1 = Point("system_metrics") \
            .tag("metric", "system_cpu_user") \
            .field("value", float(random.uniform(0, 100))) \
            .field("ems_time", ems_time) \
            .field("ram_usage", ram_usage) \
            .time(current_time, WritePrecision.S)

        point2 = Point("system_metrics") \
            .tag("metric", "system_cpu_system") \
            .field("value", float(random.uniform(0, 100))) \
            .field("ems_time", ems_time) \
            .field("ram_usage", ram_usage) \
            .time(current_time, WritePrecision.S)

        point3 = Point("system_metrics") \
            .tag("metric", "system_ram_used") \
            .field("value", float(ram_usage)) \
            .field("ems_time", ems_time) \
            .field("ram_usage", ram_usage) \
            .time(current_time, WritePrecision.S)

        data_points.append(point1)
        data_points.append(point2)
        data_points.append(point3)

        current_time += timedelta(seconds=1)  # Incrementar el tiempo en 1 segundo

    return data_points   

def generate_data(start_time, end_time):
    data_points = []
    current_time = start_time
    while current_time <= end_time:

        cpu_user = float(random.uniform(0, 100))                                # ANALIZAR PAULA
        cpu_system = float(random.uniform(0, 100))                              # ANALIZAR PAULA
        ram_used = float(random.randint(1000, 8000))                            # ANALIZAR PAULA
        # cpu_user = float(random.uniform(100, 130))                            # ANALIZAR PAULA
        # cpu_system = float(random.uniform(100, 120))                          # ANALIZAR PAULA
        # ram_used = float(random.randint(8000, 12000))                         # ANALIZAR PAULA

        point1 = Point("system_cpu_user") \
            .field("value", cpu_user) \
            .time(current_time, WritePrecision.S)

        point2 = Point("system_cpu_system") \
            .field("value", cpu_system) \
            .time(current_time, WritePrecision.S)

        point3 = Point("system_ram_used") \
            .field("value", ram_used) \
            .time(current_time, WritePrecision.S)

        data_points.append(point1)
        data_points.append(point2)
        data_points.append(point3)

        current_time += timedelta(seconds=1)                                  # ANALIZAR PAULA
        #current_time += timedelta(seconds=5)

    return data_points



if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    # Configuración de InfluxDB
    influxdb_url = "http://localhost:8086"
    influxdb_token = "my-super-secret-auth-token"
    influxdb_org = "my-org"
    influxdb_bucket = "nebulous_ApplicationPaula_bucket"                       # ANALIZAR PAULA
    #influxdb_bucket = "nebulous_Application2Paula_bucket"                       # ANALIZAR PAULA

    # Inicializar el cliente. timeout=10000 segundos
    client = InfluxDBClient(url=influxdb_url, token=influxdb_token, org=influxdb_org, timeout=10000)
    delete_api = client.delete_api()  # Para eliminar datos
    write_api = client.write_api()

    # IMPORTANT: Uncomment the following lines if you want to delete all data for the indicated metrics
    # for metric_name in ["system_cpu_system", "system_ram_used","system_cpu_user"]:
        # delete_data(delete_api, metric_name)

    # Define el rango de tiempo: 3 días atrás hasta el presente
    end_time = datetime.utcnow()  # Hora actual
    #start_time = end_time - timedelta(days=2)  # 3 días atrás                   # ANALIZAR PAULA
    start_time = end_time - timedelta(hours=3)  # 12 horas atrás               # ANALIZAR PAULA
    end_time2 = start_time + timedelta(hours=1)
    while start_time <= end_time:       
        print(f'start_time {start_time} end_time2 {end_time2} end_time {end_time}')
        data_points = generate_data(start_time, end_time2)
        write_api.write(bucket=influxdb_bucket, org=influxdb_org, record=data_points)
        print("Datos escritos en InfluxDB.")
        write_api.flush() 
        time.sleep(5)  # Espera 1 segundo entre escrituras
        start_time = end_time2
        end_time2 = end_time2 + timedelta(hours=1)
    time.sleep(5)  # Espera 1 segundo

    # exporter = InfluxDBQueryExporter(influxdb_url, influxdb_token, influxdb_org, influxdb_bucket)
    # # Configuración de la consulta
    # start_time_iso = "-1d"  # Último día
    # end_time_iso = "now()"  # Hasta el momento actual
    # configuration_file_location = "."
    # # Repetir para otras métricas
    # for metric_name in ["system_cpu_system", "system_ram_used","system_cpu_user"]:
        # exporter.query_and_save_metric(metric_name, start_time_iso, end_time_iso, configuration_file_location, print_data_from_db=True)
        # time.sleep(5)  # Espera 1 segundo

    # NO Cerrar cliente
    #client.close()
