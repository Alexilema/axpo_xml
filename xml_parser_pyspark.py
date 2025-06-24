# -*- coding: utf-8 -*-
"""
Created on Tue Jun 24 23:54:29 2025

@author: alex_

Disclaimer: this code is intended to be run in a spark enviroment, it cannot be
executed locally
"""

# Implementar paralelización con PySpark (lcon los archivos en un bucket de S3)

# Recuerda instalar las librerías necesarias (también en cada worker, más fácil
# hacerlo directamente en el cluster)

# pip install xmltodict
# pip install s3fs

# --- Aplicación de la función de parseo en paralelo ---

# IMPORTANTE: aunque ya hemos creado la sesión de S3 en el Master de Spark para acceder al
# listado de archivos, y así poder particionarlos (repartirlos entre los Workers), como lo
# que se envía a los workers no son los datos en sí sino la file key (la ruta en S3) de cada
# archivo, cada worker debe establecer de nuevo esa conexión a S3 para poder leer los archivos 

# --- map() VS. mapPartitions() ---

# - El RDD tiene elementos, en ese caso, las rutas de los archivos .xml
# - Cuando se hace una partición, se dividen esos elementos en grupos o "particiones"

# map() aplica la función a cada partición individualmente, de forma independiente.
# Cada grupo de archivos que constituyen una partición se procesa en una única tarea,
# y dentro de esa tarea se aplica la función que se le pasa a map (xml_parser_eurocontrol)
# a cada elemento de la partición (por ejemplo, usando un bucle interno). Por eso, si se usa
# map(), se establece una conexión a S3 POR CADA archivo .xml (subóptimo) (Piénsalo bien, es así)

# mapPartitions() aplica la función a toda la partición a la vez: la función que pasemos
# a mapPartitions debe ser capaz de recibir un iterador con todos los elementos de la partición,
# y devolver otro iterador. Por eso, ahora sí es posible establecer la conexión a S3 una sola vez,
# y después aplicar un "bucle for file_key in xml_files_in_partition"

## Opción a. map:
# Aplicar la función de parseo a cada archivo usando map, y descartando los resultados None
# parsed_rdd = rdd.map(xml_parser_eurocontrol).filter(lambda x: x is not None)

# NOTA: Habría que escribir código dentro de xml_parser_eurocontrol que estableciese la conexión a S3
# PERO no está porque vamos a optar por mapPartitions, y entonces la conexión se establece en otro lado

## Opción b. mapPartitions:
# La función xml_parser_eurocontrol se encapsula dentro de otra función, que primero establece
# la conexión a S3 para cada partición, y luego va parseando los archivos.

# OBSERVACIÓN:
# Una vez que ya hemos repartido los archivos entre distintos ordenadores, en cada
# ordenador podemos también hacer MultiThreading. MultiThreading y Spark van
# por separado, y se pueden combinar :)

# Libraries
import os
import datetime as dt
import boto3
import xmltodict
import pandas as pd
from pyspark.sql import SparkSession

# Faltaría definir o importar la función xml_parser

#%%
# Definir la función para mapPartitions
def process_partition(xml_files_partition):
    # Importante: importar librerías dentro de cada worker
    import os
    import boto3
    import xmltodict
    from concurrent.futures import ThreadPoolExecutor
    import traceback
     
    # Crear el cliente S3 en el worker (una sola vez para cada partición)
    session_worker = boto3.Session(aws_access_key_id = access_key,
                                   aws_secret_access_key = secret_key)
    
    s3_client_worker = session_worker.client('s3')
    
    # # Sin multithreading
    # data_list = []
    # for file_key in xml_files_partition:
    #     result = xml_parser_eurocontrol(file_key, s3_client_worker)
    #     if result is not None:
    #         data_list.append(result)

    # METER MULTITHREADING EN CADA WORKER
    # Reemplazar el bucle 'for file_key in partition' por el multithreading
    try:
        with ThreadPoolExecutor(max_workers=4) as executor:
            
            futures = []
            data_list = []  # Lista para almacenar los diccionarios
            
            for file_key in xml_files_partition:
                future = executor.submit(xml_parser, file_key, s3_client_worker)
                futures.append(future)
            
            for future in futures:
                try:
                    result = future.result()
                    if result:  # Solo agregar si el resultado no es None
                        data_list.append(result)
                    # (Se quita el pbar porque se printearían para cada worker a la vez, sería absurdo)
                except Exception as e:
                    print(f"Error en el parseado del archivo {file_key}: {e}")          

    except Exception:
        print(traceback.format_exc())
        return iter([])

    # return iter(data_list) # recuerda, debe retornar un objeto iterable (requisito de mapPartitions)
    return data_list # data_list ya lo es (da igual poner o no el iter())

#%%
# Iniciar la SparkSession
spark = SparkSession.builder.getOrCreate()

""" Aquí habría que generar la lista de xml_files, ver script 'xml_parser_s3.py' """
xml_files = ...

# --- Paralelizar y ejecutar parseado ---

# Crear un RDD con la lista, ie repartir los archivos entre los workers de Spark
rdd = spark.sparkContext.parallelize(xml_files)

# Aplicar mapPartitions al RDD:
parsed_rdd = rdd.mapPartitions(process_partition)

# Recoger los resultados en la variable data_list (*)
data_list = parsed_rdd.collect()

# (*) Nota: si los datos pasados a collect() son muy grandes, puede saturar la memoria.
# En ese caso, es mejor seguir trabajando de forma distribuida o escribir directamente
# los resultados en un sistema de archivos distribuido (como S3). Pero como ya hemos
# comentado en el script xml_parser_main.py, un df de ~5000 filas es muy pequeño

# Así pues, ya se ha terminado la lectura y parseado en paralelo de los archivos
# Ahora, el resto del código es igual a partir de esta línea:
df = pd.DataFrame(dict_list)

# Y ya se ejecuta todo en el master del cluster, no en los workers