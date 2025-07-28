from airflow import DAG                                             ## Entry point to define your workflow (DAG = Directed Acyclic Graph)
from airflow.providers.http.hooks.http import HttpHook              ## Used to interact with external APIs (e.g., fetch JSON from a REST endpoint)
from airflow.providers.postgres.hooks.postgres import PostgresHook  ## Connects to a PostgreSQL database to insert, update, or query data
from airflow.decorators import task                                 ## Converts a Python function into an Airflow task (super useful in modern DAG syntax)
from airflow.utils.dates import days_ago                            ## Sets dynamic start dates for your DAG



LATITUDE = '17.3850'       ## Latitude and longitude for the desired location (Hyderabad in this case)
LONGITUDE = '78.4867'
POSTGRES_CONN_ID = 'postgres_default'
API_CONN_ID = 'open_meteo_api'

default_args = {'owner' : 'airflow',
                'start_date' : day_ago(1)
                }

## DAG
with DAG( dag_id = 'weather_ETL_pipline',
          default_args = default_args ,
          schedule_interval = '@daily',
          catchup = False ) as drags:
    
    @task()
    def extract_weather_data():
        ##extract weather data from open-meteo API using Airflow connection

        ##to get weather data use http hoook to get data from airflow connection

        http_hook = HttpHook(http_conn_id = API_CONN_ID, method = 'GET')

        ## Build the API endpoint

        endpoint=f'/v1/forecast?latitude={LATITUDE}&longitude={LONGITUDE}&current_weather=true'
        ##request via the http hook
        response = http_hook.run(endpoint)

        if response.status_code == 200:
            return response.json()
        else:
            raise Exception(f"Failed to fetch weather data: {response.status_code}")
        
    @task()
    def trasform_weather_data(weather_data):
        cureent_weather = weather_data['current_weather']
        trasformed_data = { 'latitude' : LATITUDE,
                            'longitude' : LONGITUDE,
                            'temprature' : cureent_weather['tempreature'],
                            'windspeed': current_weather['windspeed'],
                            'winddirection': current_weather['winddirection'],
                            'weathercode': current_weather['weathercode'] }
        return transformed_data
    
    @task()
    def load_weather_data(transformed_data):
        """Load transformed data into PostgreSQL."""
        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = pg_hook.get_conn()
        cursor = conn.cursor()

        # Create table if it doesn't exist
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS weather_data (
            latitude FLOAT,
            longitude FLOAT,
            temperature FLOAT,
            windspeed FLOAT,
            winddirection FLOAT,
            weathercode INT,
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """)

        # Insert transformed data into the table
        cursor.execute("""
        INSERT INTO weather_data (latitude, longitude, temperature, windspeed, winddirection, weathercode)
        VALUES (%s, %s, %s, %s, %s, %s)
        """, (
            transformed_data['latitude'],
            transformed_data['longitude'],
            transformed_data['temperature'],
            transformed_data['windspeed'],
            transformed_data['winddirection'],
            transformed_data['weathercode']
        ))

        conn.commit()
        cursor.close()

    ## DAG Worflow- ETL Pipeline
    weather_data= extract_weather_data()
    transformed_data=transform_weather_data(weather_data)
    load_weather_data(transformed_data)