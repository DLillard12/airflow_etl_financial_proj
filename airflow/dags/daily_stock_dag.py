from airflow.models import DAG
from airflow.operators.bash import BashOperator

from datetime import datetime

dag = DAG(
    dag_id='daily_dag',
    start_date=datetime(2021, 1, 1),
    schedule="@daily"
)
with dag:
    run_script = BashOperator(
        task_id='run_script',
        bash_command='python /opt/airflow/dags/download_stock_data.py',
    )