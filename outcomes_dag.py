import os
from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

from etl_scripts.transform import transform_data
from etl_scripts.pipeline import main
from etl_scripts.cloudtopostgres import process_data_main

default_config = {
    "owner": "VineethReddy",
    "start_date": datetime(2023, 11, 1),
}

with DAG(
    dag_id="outcomes_dag",
    default_args=default_config,
    schedule_interval='@daily',
) as dag:

    extract_api_to_gcs_task = PythonOperator(
        task_id="EXTRACT_API_TO_GCS", python_callable=main,
    )

    transform_data_task = PythonOperator(
        task_id="TRANSFORM_DATA", python_callable=transform_data,
    )

    load_dim_animals_task = PythonOperator(
        task_id="LOAD_DIM_ANIMALS",
        python_callable=process_data_main,
        op_kwargs={"file_name": 'dim_animal.csv', "table_name": 'dim_animals'},
    )

    load_dim_outcome_types_task = PythonOperator(
        task_id="LOAD_DIM_OUTCOME_TYPES",
        python_callable=process_data_main,
        op_kwargs={"file_name": 'dim_outcome_types.csv', "table_name": 'dim_outcome_types'},
    )

    load_dim_dates_task = PythonOperator(
        task_id="LOAD_DIM_DATES",
        python_callable=process_data_main,
        op_kwargs={"file_name": 'dim_dates.csv', "table_name": 'dim_dates'},
    )

    load_fct_outcomes_task = PythonOperator(
        task_id="LOAD_FCT_OUTCOMES",
        python_callable=process_data_main,
        op_kwargs={"file_name": 'fct_outcomes.csv', "table_name": 'fct_outcomes'},
    )


    extract_api_to_gcs_task >> transform_data_task >> [
        load_dim_animals_task,
        load_dim_outcome_types_task,
        load_dim_dates_task,
        load_fct_outcomes_task,
    ]
