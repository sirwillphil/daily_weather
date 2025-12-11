import requests
import json
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

# Define what your task does
def get_weather():
    url = "https://api.open-meteo.com/v1/forecast?latitude=47.6668&longitude=-122.29&hourly=temperature_2m&current=temperature_2m&forecast_days=1"

    respone = requests.get(url)
    data = respone.json()

    raw_path = "/opt/airflow/data/raw/weather_raw.json"
    with open(raw_path, "w") as f:
        json.dump(data, f)

def process_weather():
    raw_path = "/opt/airflow/data/raw/weather_raw.json"
    processed_path = "/opt/airflow/data/processed/weather_clean.csv"

    with open(raw_path, "r") as f:
        raw_data = json.load(f)
    
    df = pd.DataFrame({
        "timestamp": "This is a test"
    })

    df.to_csv(processed_path, index=False)
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
        task_id='my_task',
        python_callable=get_weather,
    )

    task2 = PythonOperator(
        task_id='process_weather',
        python_callable=process_weather,
    )

    task1 >> task2