# Daniel Lillard
# 2025.05.14
# This is my hello world for airflow, tutorial found here: https://learn.microsoft.com/en-us/fabric/data-factory/apache-airflow-jobs-hello-world

from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'owner':'airflow',
    'depends_on_past':False,
    'start_date':datetime(2025,5,14),
    'email_on_failure':False,
    'email_on_retry':False,
    'retries':1
}

# instantiate the dag object
with DAG(
    'hello_world_dag',
    default_args = default_args,
    description='A simple hello world, my first DAG in Airflow!',
    schedule=None,
    catchup=False,
) as dag:

    hello_task = BashOperator(
        task_id = 'hello_world_task',
        bash_command = 'echo "Hello World!"'
    )

hello_task