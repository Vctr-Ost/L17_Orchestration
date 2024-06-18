from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import requests

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 5, 1),
    # 'email': ['your_email@example.com'],
    # 'email_on_failure': False,
    # 'email_on_retry': False,
    'retries': 2,
    'retry_delay': 5,
}

# Initialize the DAG
dag = DAG(
    'process_sales',
    default_args=default_args,
    description='process_sales DAG',
    schedule_interval=None,
)

# Python functions to be used in the tasks
def loader_GCS_to_GBQ():
    uri = 'http://host.docker.internal:8081/gcs-to-gbq/sales/False'
    resp = requests.get(uri)
    print(resp.status_code)

def bronze_to_silver():
    uri = 'http://host.docker.internal:8082/sales'
    resp = requests.get(uri)
    print(resp.status_code)

# Define the tasks
task1 = PythonOperator(
    task_id='GCS_to_GBQ',
    python_callable=loader_GCS_to_GBQ,
    dag=dag,
)

task2 = PythonOperator(
    task_id='bronze_to_silver',
    python_callable=bronze_to_silver,
    dag=dag,
)

# Set the task dependencies
task1 >> task2
