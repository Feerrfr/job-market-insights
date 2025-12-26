import requests
import streamlit as st
import pandas
from deltalake import write_deltalake, DeltaTable
from deltalake.exceptions import TableNotFoundError
import os  # <--- Nuevo: Para leer el sistema operativo
from dotenv import load_dotenv  # <--- Nuevo: Para leer el archivo .env
import snowflake

load_dotenv()

ADZUNA_APP_ID = os.getenv('ADZUNA_APP_ID')
ADZUNA_APP_KEY = os.getenv('ADZUNA_APP_KEY')


def get_data(base_url, endpoint, data_field=None, params=None, headers=None):
    """
    Realiza una solicitud GET a una API para obtener datos.

    Parámetros:
    base_url (str): La URL base de la API.
    endpoint (str): El endpoint de la API al que se realizará la solicitud.
    data_field (str): Atribudo del json de respuesta donde estará la lista
    de objetos con los datos que requerimos
    params (dict): Parámetros de consulta para enviar con la solicitud.
    headers (dict): Encabezados para enviar con la solicitud.

    Retorna:
    dict: Los datos obtenidos de la API en formato JSON.
    """
    try:
        endpoint_url = f"{base_url}/{endpoint}"
        response = requests.get(endpoint_url, params=params, headers=headers)
        response.raise_for_status()  # Levanta una excepción si hay un error en la respuesta HTTP.

        # Verificar si los datos están en formato JSON.
        try:
            data = response.json()
            if data_field:
              data = data[data_field]
        except:
            print("El formato de respuesta no es el esperado")
            return None
        return data
    except requests.exceptions.RequestException as e:
        # Capturar cualquier error de solicitud, como errores HTTP.
        print(f"La petición ha fallado. Código de error : {e}")
        print(f"Solicitar API_KEY válida si el error persiste.")
        print(f"Saliendo del script...")
        exit()
        return None

country= "es"

ids= f"/1?app_id={ADZUNA_APP_ID}&app_key={ADZUNA_APP_KEY}&results_per_page=200&what_phrase=data%20engineer"
url_base= f"https://api.adzuna.com/v1/api"
endpoint= f"jobs/{country}/search{ids}"

def probar_api():
    st.write("hola mundo")
    usuario_json = get_data(url_base, endpoint)
    df= pandas.DataFrame(usuario_json)
    
    print(df)
    st.write(df)

def subir_empleos(df):
    conn = snowflake.connector.connect(
        user= os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        warehouse='COMPUTE_HW',
        database= 'JOB_MARKET_DB',
        schema='BRONZE'
    )
    
    try:
        cursor = conn.cursor()
        cursor.execute(f"USE WAREHOUSE COMPUTE_WH")
        cursor.execute(f"USE DATABASE JOB_MARKET_DB")
        cursor.execute(f"USE SCHEMA BRONZE")

        _,_,nrows,_ = write_pandas(conn, df,_,auto_create_table=True)
        print(f"Datos cargados en Snowflake: {nrow}")         
    except Exception as e:
        print(f"💥 Error en Snowflake: {e}")
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":

    st.set_page_config( layout="wide")
    df=probar_api()
    subir_empleos(df)