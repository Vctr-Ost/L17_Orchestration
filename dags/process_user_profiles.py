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
    'process_user_profiles',
    default_args=default_args,
    description='process_user_profiles DAG',
    schedule_interval=None,
)

# Python functions to be used in the tasks
def loader_GCS_to_GBQ():
    resp = requests.get('http://host.docker.internal:8081/gcs-to-gbq/user_profiles/false')
    print(resp.status_code)

# Define the tasks
task1 = PythonOperator(
    task_id='GCS_to_GBQ',
    python_callable=loader_GCS_to_GBQ,
    dag=dag,
)


# Set the task dependencies
task1
