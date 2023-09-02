import datetime as dt
from pathlib import Path
import pandas as pd
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

dag = DAG(
    dag_id="08_templated_path_5",
    schedule_interval="@daily",
    start_date=dt.datetime(year=2018, month=12, day=31),
    end_date=dt.datetime(year=2019, month=1, day=4),
    tags=["user_events"]
)

fetch_events = BashOperator(
    task_id="fetch_events",
    bash_command="""
    curl -o /home/ubuntu/Downloads/data/partition_data/events_noformat_partition.json -L 'http://127.0.0.1:5000/events?start_date={{ds}}&end_date={{next_ds}}';
    python -m json.tool < /home/ubuntu/Downloads/data/partition_data/events_noformat_partition.json > /home/ubuntu/Downloads/data/partition_data/events_{{ds}}.json;
    """,
    dag=dag,
)

def _calculate_stats(**context):
    """Calculates event statistics."""
    input_path = context["templates_dict"]["input_path"] 
    output_path = context["templates_dict"]["output_path"]

    events = pd.read_json(input_path) 
    stats = events.groupby(["date", "user"]).size().reset_index()
    stats.columns = ["date", "user", "stats"]  # Set column names
    Path(output_path).parent.mkdir(exist_ok=True) 
    stats.to_csv(output_path, index=False) 


calculate_stats = PythonOperator(
    task_id="calculate_stats",
    python_callable=_calculate_stats,
    templates_dict={
    "input_path": "/home/ubuntu/Downloads/data/partition_data/events_{{ds}}.json",
    "output_path": "/home/ubuntu/Downloads/data/partition_data/stats_{{ds}}.csv",
    },
    dag=dag,
)

fetch_events >> calculate_stats