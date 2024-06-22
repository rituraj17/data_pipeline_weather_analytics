### Airflow DAG Setup with Astro CLI 🚀

#### Project Overview

This repository contains an Airflow DAG (`postgres_extract_data_dag_with_api_v1.py`) that orchestrates the extraction of weather data from an API, transformation, loading into a PostgreSQL database, and subsequent retrieval and transformation back to a DataFrame. This README provides an overview of the repository structure, how to set up Airflow with Astro CLI, and details on running and testing the DAG.

#### Repository Structure

```
├── README.md                  # This README file
├── airflow_settings.yaml      # Configuration file for Airflow settings (if any)
├── dags                       # Directory containing Airflow DAG scripts
│   ├── postgres_extract_data_dag_with_api_v1.py  # Main DAG orchestrating data extraction and transformation
├── include                    # Directory for any additional includes (if needed)
├── plugins                    # Directory for Airflow plugins (if any)
└── tests                      # Directory for Airflow DAG tests
    └── dags
        └── test_dag_example.py  # Example test script for Airflow DAGs (placeholder)
```

#### Prerequisites

1. **Astro CLI Installation**:
   - Install Astro CLI following the instructions from [Astro CLI GitHub](https://github.com/astronomer/astro-cli).

2. **PostgreSQL Setup**:
   - Ensure you have a PostgreSQL database running and accessible. Update connection details in the DAG (`postgres_extract_data_dag_with_api_v1.py`) accordingly.

#### Airflow DAG Description

The main Airflow DAG (`dags/postgres_extract_data_dag_with_api_v1.py`) orchestrates the following tasks:

1. **Create Table If Not Exists (`create_table_if_not_exists`)**:
   - Ensures the `weather_data` table exists in the PostgreSQL database. If not, it creates the table with columns `date`, `location`, `temp`, and `conditions`.

2. **Extract Data from API (`extract_data`)**:
   - Simulates extracting weather data from an API. Generates 10 records with random dates, locations, temperatures, and weather conditions.

3. **Insert Data into PostgreSQL (`insert_data`)**:
   - Inserts the extracted weather data into the `weather_data` table in PostgreSQL.

4. **Fetch Data from Database (`fetch_data_from_database`)**:
   - Selects all data from the `weather_data` table in PostgreSQL.

5. **Transform Data (`transform_steps`)**:
   - Transforms the fetched data into a format suitable for loading into a DataFrame.

6. **Load Data into DataFrame (`load_data_to_dataframe`)**:
   - Loads the transformed data into a Pandas DataFrame and prints it.

#### Astro CLI Setup Steps

1. **Initialize Astro Project**:
   - Initialize a new Astro project in your repository:

     ```bash
     astro airflow init
     ```

2. **Install Dependencies**:
   - Ensure required dependencies (like `pandas` and `psycopg2`) are listed in `requirements.txt`.

3. **Deploy DAG**:
   - Copy the DAG file (`postgres_extract_data_dag_with_api_v1.py`) into the `dags` directory of your Astro project.

4. **Start Airflow Services**:
   - Start Airflow services using Astro CLI:

     ```bash
     astro airflow start
     ```

5. **Access Airflow Web UI**:
   - Access the Airflow Web UI by navigating to `http://localhost:8080` in your web browser.

#### Running the DAG

- The DAG `postgres_extract_data_dag_with_api_v1` should appear in the Airflow Web UI.
- Trigger the DAG manually or enable the schedule as per your requirement (`start_date`, `schedule` parameters in the DAG definition).

#### Testing

- This repository includes a placeholder for DAG tests (`tests/dags/test_dag_example.py`). Add unit tests using libraries like `unittest` or `pytest` to test specific functions or components of your DAG.
