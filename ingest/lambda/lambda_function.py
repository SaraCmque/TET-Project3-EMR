import boto3
import requests
import json
import pymysql
import pandas as pd
from io import StringIO
from datetime import datetime
 
print('Loading function')
 
def lambda_handler(event, context):
    latitude = 6.25
    longitude = -75.56
    start_date = "2022-01-01"
    end_date = "2022-12-31"
    timezone = "America/Bogota"
 
    host = "climate-db.c2hyrnbdeokg.us-east-1.rds.amazonaws.com"
    user = "admin"
    password = "Climate2025!"
    database = "climate_data"
    port = 3306
 
    def download_weather_data():
        url = f"https://archive-api.open-meteo.com/v1/archive?latitude={latitude}&longitude={longitude}&start_date={start_date}&end_date={end_date}&daily=temperature_2m_max,precipitation_sum&timezone={timezone}"
        response = requests.get(url)
        data = response.json()
        return data
 
    def upload_weather_data_to_s3(data):
        file_name = f"openmeteo_medellin_{start_date}_{end_date}.json"
        json_data = json.dumps(data)
       
        s3 = boto3.client("s3")
        bucket_name = "eafit-project-3-bucket"
        s3.put_object(
            Bucket=bucket_name,
            Key=f"raw/api/{file_name}",
            Body=json_data
        )
        print(f"Datos climáticos subidos a s3://{bucket_name}/raw/api/{file_name}")
 
    def export_traffic_data_to_s3():
        db_config = {
            'host': host,
            'user': user,
            'password': password,
            'database': database,
            'port': port
        }
       
        try:
            connection = pymysql.connect(**db_config)
            query = "SELECT * FROM traffic_incidents"
            df = pd.read_sql(query, connection)
           
            csv_buffer = StringIO()
            df.to_csv(csv_buffer, index=False)
           
            s3 = boto3.client('s3')
            bucket_name = "eafit-project-3-bucket"
            s3_key = f"raw/mysql/traffic_incidents_{start_date}_{end_date}.csv"
            s3.put_object(Bucket=bucket_name, Key=s3_key, Body=csv_buffer.getvalue())
           
            print(f"Datos de tráfico subidos a s3://{bucket_name}/{s3_key}")
           
        except Exception as e:
            print(f"Error al exportar datos de MySQL: {str(e)}")
            raise
        finally:
            if 'connection' in locals() and connection.open:
                connection.close()
 
    print(f"Iniciando ingesta automática: {datetime.now()}")
   
    try:
        weather_data = download_weather_data()
        upload_weather_data_to_s3(weather_data)
        export_traffic_data_to_s3()
        print(f"Ingesta completada correctamente: {datetime.now()}")
        return {
            'statusCode': 200,
            'body': json.dumps('Ingesta completada exitosamente')
        }
    except Exception as e:
        print(f"Error general en la ingesta: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps(f'Error en la ingesta: {str(e)}')
        }
 