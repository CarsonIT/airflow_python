import datetime as dt
from pathlib import Path
import pandas as pd
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

dag = DAG(
    dag_id="03_with_end_date",
    start_date=dt.datetime(year=2023, month=8, day=1),
    end_date=dt.datetime(year=2023, month=8, day=5),
    schedule_interval="@daily", 
    tags=["user_events"]
)

fetch_events = BashOperator(
    task_id="fetch_events",
    bash_command="""
    curl -o /home/ubuntu/Downloads/data/events_noformat.json -L 'http://127.0.0.1:5000/events';
    python -m json.tool < /home/ubuntu/Downloads/data/events_noformat.json > /home/ubuntu/Downloads/data/events.json;
    """,
    dag=dag,
)

def _calculate_stats(input_path, output_path):
    """Calculates event statistics."""
    events = pd.read_json(input_path) 
    stats = events.groupby(["date", "user"]).size().reset_index()
    stats.columns = ["date", "user", "stats"]  # Set column names
    Path(output_path).parent.mkdir(exist_ok=True) 
    stats.to_csv(output_path, index=False) 


calculate_stats = PythonOperator(
    task_id="calculate_stats",
    python_callable=_calculate_stats,
    op_kwargs={
    "input_path": "/home/ubuntu/Downloads/data/events.json",
    "output_path": "/home/ubuntu/Downloads/data/stats.csv",
    },
    dag=dag,
)

fetch_events >> calculate_stats