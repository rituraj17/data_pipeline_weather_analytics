from airflow.decorators import dag, task
from datetime import datetime, timedelta
from airflow.providers.postgres.hooks.postgres import PostgresHook
import sys
import random

# from airflow import DAG
from datetime import datetime

# from airflow.operators.python import PythonOperator
import pandas as pd


@dag(
    dag_id="postgres_extract_data_dag_with_api_v1",
    # default_args=default_args,
    start_date=datetime(year=2024, month=1, day=1, hour=9, minute=0),
    # schedule="0 0 * * *",
    catchup=False,
    max_active_runs=1,
    render_template_as_native_obj=True,
)
def start_dag_process():

    @task(task_id="create_table_if_not_exists")
    def create_table_if_not_exists():
        try:
            print("Starting checking for postgres connection")
            postgres_hook = PostgresHook(postgres_conn_id="postgres")
            create_table_sql = """
            CREATE TABLE IF NOT EXISTS weather_data (
                date DATE,
                location VARCHAR(50),
                temp INT,
                conditions VARCHAR(255)
            );
            """
            postgres_hook.run(create_table_sql)
        except Exception as e:
            print(e)
            raise

    @task(task_id="extract_data")
    def extract_data_callable(name, age):
        print(f"Extracting data from a weather API {name} which is {age} years old")

        # Generate 10 data records
        base_date = datetime(2023, 1, 1)
        locations = ["NYC", "Patna", "London", "Tokyo", "Sydney", "Moscow", "Paris", "Berlin", "Mumbai", "Cape Town"]
        conditions = [
            "Sunny",
            "Cloudy",
            "Rainy",
            "Snowy",
            "Windy",
            "Foggy",
            "Thunderstorm",
            "Drizzle",
            "Clear",
            "Partly Cloudy",
        ]

        data = []
        for i in range(10):
            record_date = (base_date + timedelta(days=i)).strftime("%Y-%m-%d")
            record_location = locations[i % len(locations)]
            record_temp = random.randint(-10, 40)  # Random temperature between -10 and 40 degrees
            record_conditions = random.choice(conditions)

            data.append(
                {
                    "date": record_date,
                    "location": record_location,
                    "weather": {"temp": record_temp, "conditions": record_conditions},
                }
            )

        return data

    @task(task_id="insert_data")
    def insert_data(records: list):
        postgres_hook = PostgresHook(postgres_conn_id="postgres")
        insert_sql = """
        INSERT INTO weather_data (date, location, temp, conditions)
        VALUES (%s, %s, %s, %s);
        """
        for record in records:
            postgres_hook.run(
                insert_sql,
                parameters=(
                    record["date"],
                    record["location"],
                    record["weather"]["temp"],
                    record["weather"]["conditions"],
                ),
            )

    @task(task_id="fetch_data_from_database")
    def select_data_from_table():
        postgres_hook = PostgresHook(postgres_conn_id="postgres")
        select_sql = "SELECT date, location, temp, conditions FROM weather_data;"
        connection = postgres_hook.get_conn()
        cursor = connection.cursor()
        cursor.execute(select_sql)
        rows = cursor.fetchall()
        cursor.close()
        connection.close()

        # Transforming into list of dicts
        columns = ["date", "location", "temp", "conditions"]
        result = [dict(zip(columns, row)) for row in rows]
        return result

    @task(task_id="transform_steps")
    def transform_data_callable(raw_data):
        # Transform response to a list
        transformed_data = []
        for i in raw_data:
            temp_data = (
                i.get("date"),
                i.get("location"),
                i.get("temp"),
                i.get("conditions"),
            )
            print(temp_data)
            transformed_data.append(temp_data)
        return transformed_data

    @task(task_id="load_data_to_dataframe")
    def load_data_callable(transformed_data):
        # Load the data to a DataFrame, set the columns
        loaded_data = pd.DataFrame(transformed_data)
        loaded_data.columns = ["date", "location", "weather_temp", "weather_conditions"]
        print(loaded_data)

    # # Set dependencies between tasks
    extract_data = extract_data_callable(name="google", age="1")
    check_postgres = create_table_if_not_exists()
    insert_data_step = insert_data(extract_data)
    extract_data_database = select_data_from_table()
    transform_data = transform_data_callable(extract_data_database)
    load_data = load_data_callable(transform_data)

    # Ensure the table creation happens before data extraction and insertion
    check_postgres >> extract_data >> insert_data_step >> extract_data_database >> transform_data >> load_data


start_dag = start_dag_process()
