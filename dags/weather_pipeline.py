import json
import requests

from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator

from settings.base import OPEN_METEO_URL, CITIES, PG_CONN_ID

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
}

dag = DAG(
    "weather_etl",
    default_args=default_args,
    description="ETL pipeline for Open-Meteo weather data",
    schedule_interval="@daily",
    catchup=False
)


def fetch_weather_data(ti):
    """Fetch weather data from Open-Meteo API for predefined cities and store in XCom."""
    request_records = []
    weather_records = []

    for city, coords in CITIES.items():
        yesterday = datetime.now() - timedelta(days=1)
        date_str = yesterday.strftime('%Y-%m-%d')

        params = {
            "latitude": coords["lat"],
            "longitude": coords["lon"],
            "hourly": "temperature_2m,wind_speed_10m,precipitation",
            "timezone": "America/New_York",
            "start_date": date_str,
            "end_date": date_str
        }

        response = requests.get(OPEN_METEO_URL, params=params)
        data = response.json()

        request_record = {
            "timestamp": str(datetime.utcnow()),
            "city": city,
            "latitude": coords["lat"],
            "longitude": coords["lon"]
        }
        request_records.append(request_record)

        for i, hour in enumerate(data["hourly"]["time"]):
            weather_records.append({
                "hour": hour,
                "temperature": data["hourly"]["temperature_2m"][i],
                "wind_speed": data["hourly"]["wind_speed_10m"][i],
                "precipitation": data["hourly"]["precipitation"][i],
                "city": city
            })

    ti.xcom_push(key="request_records", value=json.dumps(request_records))
    ti.xcom_push(key="weather_records", value=json.dumps(weather_records))


def insert_weather_data(ti):
    """Insert weather data into PostgreSQL."""
    hook = PostgresHook(postgres_conn_id=PG_CONN_ID)
    conn = hook.get_conn()
    cursor = conn.cursor()

    # Insert weather request metadata
    request_records = json.loads(ti.xcom_pull(key="request_records"))
    for record in request_records:
        cursor.execute("""
            INSERT INTO weather_requests (timestamp, city, latitude, longitude)
            VALUES (%s, %s, %s, %s)
            RETURNING request_id;
        """, (record["timestamp"], record["city"], record["latitude"], record["longitude"]))
        request_id = cursor.fetchone()[0]

        # Insert corresponding weather data
        weather_records = json.loads(ti.xcom_pull(key="weather_records"))
        for weather in weather_records:
            if weather["city"] == record["city"]:
                cursor.execute("""
                    INSERT INTO weather_data (request_id, hour, temperature, wind_speed, precipitation)
                    VALUES (%s, %s, %s, %s, %s);
                """, (
                    request_id, weather["hour"], weather["temperature"], weather["wind_speed"],
                    weather["precipitation"]))

    conn.commit()
    cursor.close()
    conn.close()


fetch_weather = PythonOperator(
    task_id="fetch_weather",
    python_callable=fetch_weather_data,
    provide_context=True,
    dag=dag
)

insert_weather_data_task = PythonOperator(
    task_id="insert_weather_data",
    python_callable=insert_weather_data,
    provide_context=True,
    dag=dag
)

fetch_weather >> insert_weather_data_task
