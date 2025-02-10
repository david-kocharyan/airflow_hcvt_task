# Weather Data Pipeline

This project implements a data pipeline that extracts hourly weather data from the Open-Meteo API for New York City, Los Angeles, and San Francisco. The extracted data is then transformed and loaded into a PostgreSQL database. The pipeline is automated using Apache Airflow to fetch the latest weather data every 24 hours.

## Setup Instructions

### 1. Install Requirements

Ensure you have Python installed and a virtual environment created, then install the required dependencies:

```sh
pip install -r requirements.txt
```

### 2. Set Up Apache Airflow

Set the Airflow home directory:

```sh
export AIRFLOW_HOME=/your/path
```

Initialize the Airflow database:

```sh
airflow db init
```

Create an Airflow superuser:

```sh
airflow users create \
    --username admin \
    --firstname admin \
    --lastname admin \
    --email admin@gmail.com \
    --role Admin \
    --password 123456
```

### 3. Configure Airflow Variables

Airflow Variables store dynamic values like API URLs, city coordinates, and database connection identifiers.

#### Steps to Add Variables in Airflow UI:
1. Navigate to **Airflow UI** → **Admin** → **Variables**.
2. Click **"Create"** and add the following:

| Key               | Value (JSON Format) |
|------------------|-------------------|
| `OPEN_METEO_URL` | `"https://api.open-meteo.com/v1/forecast"` |

### 4. Configure Airflow Connection (PostgreSQL)

1. Navigate to **Airflow UI** → **Admin** → **Connections**.
2. Click **"Create"**, and enter:
   - **Connection Id**: `postgres_hcvt`
   - **Connection Type**: `Postgres`
   - **Host**: `localhost`
   - **Schema**: `hcvt`
   - **Login**: `postgres`
   - **Password**: `root`
   - **Port**: `5432`
3. Save the connection.

### 5. Set Up PostgreSQL Database

Start PostgreSQL and create the required database:

```sh
psql -U your_username -d your_database -h localhost -p 5432
```

Run the following SQL commands to create the necessary tables:

```sql
CREATE TABLE IF NOT EXISTS weather_requests (
    request_id SERIAL PRIMARY KEY,
    timestamp TIMESTAMP NOT NULL,
    city TEXT NOT NULL,
    latitude DOUBLE PRECISION NOT NULL,
    longitude DOUBLE PRECISION NOT NULL
);

CREATE TABLE IF NOT EXISTS weather_data (
    data_id SERIAL PRIMARY KEY,
    request_id INT REFERENCES weather_requests(request_id) ON DELETE CASCADE,
    hour TIMESTAMP NOT NULL,
    temperature DOUBLE PRECISION NOT NULL,
    wind_speed DOUBLE PRECISION NOT NULL,
    precipitation DOUBLE PRECISION NOT NULL
);
```

### 6. Run the Airflow Scheduler and Web Server

Start the Airflow scheduler:

```sh
airflow scheduler
```

Start the Airflow web server:

```sh
airflow webserver --port 8080
```