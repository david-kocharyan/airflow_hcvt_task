from airflow.models import Variable

OPEN_METEO_URL = Variable.get("OPEN_METEO_URL", default_var="https://api.open-meteo.com/v1/forecast")

CITIES = {
    "New York City": {"lat": 40.712776, "lon": -74.005974},
    "Los Angeles": {"lat": 34.052235, "lon": -118.243683},
    "San Francisco": {"lat": 37.774929, "lon": -122.419418}
}

PG_CONN_ID = "postgres_hcvt"
