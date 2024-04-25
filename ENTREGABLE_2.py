import requests
import psycopg2
url="data-engineer-cluster.cyhh5bfevlmn.us-east-1.redshift.amazonaws.com"
data_base="data-engineer-database"
user="giulylucente_coderhouse"
def conectar_bd():
    try:
        connection = psycopg2.connect(
            host='data-engineer-cluster.cyhh5bfevlmn.us-east-1.redshift.amazonaws.com',
            dbname=data_base,
            user=user,
            password='pwd',
            port='5439'
        )
        print("Conectado a Redshift con éxito!")
        return connection

    except Exception as e:
        print("No es posible conectar a Redshift")
        print(e)
        return None  # Devolver None en caso de error

# Definir la función para extraer datos de la API
def obtener_datos_desde_api(url):
    response = requests.get(url)
    if response.status_code == 200:
        return response.json()
    else:
        print("Error al obtener datos de la API")
        return None

# Ejecutar la función de obtener datos desde la API
url_api = "https://eonet.gsfc.nasa.gov/api/v3/events"
datos_api = obtener_datos_desde_api(url_api)


# Definir la función para crear la tabla en la base de datos
def crear_tabla(connection):
    try:
        with connection.cursor() as cur:
            cur.execute("""
                    CREATE TABLE IF NOT EXISTS EVENTS_NASA (
                        id int PRIMARY KEY,
                        titulo TEXT,
                        descripcion TEXT,
                        fecha_inicio TIMESTAMP,
                        fecha_fin TIMESTAMP,
                        categoria TEXT,
                        creado_en TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
            """)
            connection.commit()
        print("Tabla creada exitosamente")
    except (Exception, psycopg2.Error) as error:
        print("Error al crear la tabla:", error)


# Definir la función para insertar datos en la tabla
def insertar_datos(connection, datos):
    try:
        cursor = connection.cursor()
        insert_query = '''
            INSERT INTO EVENTS_NASA (id, titulo, descripcion, fecha_inicio, fecha_fin, categoria)
            VALUES (%s, %s, %s, %s, %s, %s);
        '''
        for i, evento in enumerate(datos['events']):
            cursor.execute(insert_query, (
                i + 1,
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

conn = conectar_bd()
if conn is not None:  # Verificar si la conexión no es None
    # Crear la tabla en la base de datos
    crear_tabla(conn)


    if datos_api:
        # Insertar datos en la tabla
        insertar_datos(conn, datos_api)

    # Cerrar la conexión a la base de datos
    conn.close()
else:
    print("No se pudo establecer conexión a la base de datos.")
