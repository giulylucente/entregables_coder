import requests
import json
import psycopg2
from psycopg2 import sql


# Definir la función para extraer datos de la API
def obtener_datos_desde_api(url):
    response = requests.get(url)
    if response.status_code == 200:
        return response.json()
    else:
        print("Error al obtener datos de la API")
        return None


# Definir la función para crear la conexión a la base de datos
def conectar_bd():
    try:
        connection = psycopg2.connect(
            user="postgres",
            password="pwd_tuya",
            host="localhost",
            port="5432",
            database="EVENTS_NASA"
        )
        return connection
    except (Exception, psycopg2.Error) as error:
        print("Error al conectar a la base de datos:", error)
        return None


# Definir la función para crear la tabla en la base de datos
def crear_tabla(connection):
    try:
        cursor = connection.cursor()
        create_table_query = '''
            CREATE TABLE IF NOT EXISTS eventos (
                id SERIAL PRIMARY KEY,
                titulo TEXT,
                descripcion TEXT,
                fecha_inicio TIMESTAMP,
                fecha_fin TIMESTAMP,
                categoria TEXT,
                creado_en TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        '''
        cursor.execute(create_table_query)
        connection.commit()
        print("Tabla creada exitosamente")
    except (Exception, psycopg2.Error) as error:
        print("Error al crear la tabla:", error)


# Definir la función para insertar datos en la tabla
def insertar_datos(connection, datos):
    try:
        cursor = connection.cursor()
        insert_query = '''
            INSERT INTO eventos (titulo, descripcion, fecha_inicio, fecha_fin, categoria)
            VALUES (%s, %s, %s, %s, %s);
        '''
        for evento in datos['events']:
            cursor.execute(insert_query, (
                evento['title'],
                evento['description'],
                evento['geometry'][0]['date'],
                evento['geometry'][-1]['date'],
                evento['categories'][0]['title']
            ))
        connection.commit()
        print("Datos insertados exitosamente")
    except (Exception, psycopg2.Error) as error:
        print("Error al insertar datos:", error)


# URL de la API
url_api = "https://eonet.gsfc.nasa.gov/api/v3/events"

# Obtener datos de la API
datos_api = obtener_datos_desde_api(url_api)

# Conectar a la base de datos
conexion_bd = conectar_bd()

if conexion_bd:
    # Crear la tabla en la base de datos
    crear_tabla(conexion_bd)

    if datos_api:
        # Insertar datos en la tabla
        insertar_datos(conexion_bd, datos_api)

    # Cerrar la conexión a la base de datos
    conexion_bd.close()



