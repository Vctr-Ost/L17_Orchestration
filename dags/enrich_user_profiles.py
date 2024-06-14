from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import requests

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 5, 1),
    'retries': 2,
    'retry_delay': 5,
}

# Initialize the DAG
dag = DAG(
    'enrich_user_profiles',
    default_args=default_args,
    description='enrich_user_profiles DAG',
    schedule_interval=None,
)

# Python functions to be used in the tasks
def customers_enricher():
    resp = requests.get('http://host.docker.internal:8082/customers_enrich')
    print(resp.status_code)


def gold_layer_transporter():
    resp = requests.get('http://host.docker.internal:8083/enrich_user_profiles')
    print(resp.status_code)


# Define the tasks
task1 = PythonOperator(
    task_id='GCS_to_GBQ',
    python_callable=customers_enricher,
    dag=dag,
)

task2 = PythonOperator(
    task_id='Gold_layer_transporter',
    python_callable=gold_layer_transporter,
    dag=dag,
)


# Set the task dependencies
task1 >> task2
