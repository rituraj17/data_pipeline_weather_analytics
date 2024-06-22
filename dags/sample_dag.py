from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator
import pandas as pd

with DAG(
    dag_id="weather_etl_v4",
    start_date=datetime(year=2024, month=1, day=1, hour=9, minute=0),
    schedule="@daily",
    catchup=True,
    max_active_runs=1,
    render_template_as_native_obj=True,
) as dag:

    def extract_data_callable(name, age):
        print(f"my name is {name} and age is {age} old.")
        # Print message, return a response
        print("Extracting data from an weather API")
        return [
            {"date": "2023-01-01", "location": "NYC", "weather": {"temp": 33, "conditions": "Light snow and wind"}},
            {
                "date": "2023-01-01",
                "location": "patna",
                "weather": {"temp": 44, "conditions": "sunny"},
            },
        ]

    extract_data = PythonOperator(
        dag=dag,
        task_id="extract_data",
        python_callable=extract_data_callable,
        op_kwargs={"name": "Rituraj", "age": "27"},
    )

    def transform_data_callable(raw_data):
        # Transform response to a list
        transformed_data = []
        for i in raw_data:
            temp_data = (
                i.get("date"),
                i.get("location"),
                i.get("weather").get("temp"),
                i.get("weather").get("conditions"),
            )
            print(temp_data)
            transformed_data.append(temp_data)
        return transformed_data

    transform_data = PythonOperator(
        dag=dag,
        task_id="transform_data",
        python_callable=transform_data_callable,
        op_kwargs={"raw_data": "{{ ti.xcom_pull(task_ids='extract_data') }}"},
    )

    def load_data_callable(transformed_data):
        # Load the data to a DataFrame, set the columns
        loaded_data = pd.DataFrame(transformed_data)
        loaded_data.columns = ["date", "location", "weather_temp", "weather_conditions"]
        print(loaded_data)

    load_data = PythonOperator(
        dag=dag,
        task_id="load_data",
        python_callable=load_data_callable,
        op_kwargs={"transformed_data": "{{ ti.xcom_pull(task_ids='transform_data') }}"},
    )

    # Set dependencies between tasks
    extract_data >> transform_data >> load_data
