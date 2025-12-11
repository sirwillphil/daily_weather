import os
import requests
import json
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

BASE_DIR = "/opt/airflow/"

# Define what your task does
def get_weather():
    url = "https://api.open-meteo.com/v1/forecast?latitude=47.6668&longitude=-122.29&current=temperature_2m,relative_humidity_2m&forecast_days=1"

    respone = requests.get(url)
    data = respone.json()

    raw_path = os.path.join(BASE_DIR, "data/raw/weather_raw.json")
    os.makedirs(os.path.dirname(raw_path), exist_ok=True)
    with open(raw_path, "w") as f:
        json.dump(data, f)

def process_weather():
    raw_path = os.path.join(BASE_DIR, "data/raw/weather_raw.json")
    processed_path = os.path.join(BASE_DIR, "data/processed/weather_clean.csv")
    os.makedirs(os.path.dirname(processed_path), exist_ok=True)

    with open(raw_path, "r") as f:
        raw_data = json.load(f)

    print(raw_data)
    
    df = pd.DataFrame([{
        'time': raw_data['current']['time'],
        'temperature_2m': raw_data['current']['temperature_2m'],
        'relative_humidity_2m': raw_data['current']['relative_humidity_2m'],
        'ingestion_time': datetime.now().isoformat()
    }])
    df.to_csv(processed_path, mode='a', header=not os.path.exists(processed_path), index=False)
    print(f"Saved processed CSV to {processed_path}")


# Create the DAG
with DAG(
    dag_id='weather',              # Change this to your DAG name
    start_date=datetime(2025, 12, 11),   # When DAG becomes active
    schedule='@hourly',                  # How often it runs
    catchup=False,
    tags=['weather'],                      # Don't run for past dates
) as dag:
    
    # Create a task
    task1 = PythonOperator(
        task_id='get_weather',
        python_callable=get_weather,
    )

    task2 = PythonOperator(
        task_id='process_weather',
        python_callable=process_weather,
    )

    task1 >> task2