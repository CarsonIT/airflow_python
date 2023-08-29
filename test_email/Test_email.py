
import datetime
from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator

default_args = {
    'retries': 1,
    'retry_delay': timedelta(seconds=5),
    'email': ['carsonit01@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': True
}

with DAG(
    dag_id="test_email",
    start_date=datetime(2023, 8, 28),
    catchup=False,
    tags=["test_email"],
    schedule_interval=None,
    default_args = default_args
) as dag:
    task1 = BashOperator (
        task_id="fetch_events",
        bash_command="""cd non extis folder""",
        dag=dag,
    )

