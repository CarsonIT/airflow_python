import logging
import os
import json
from dotenv import load_dotenv
from kafka.consumer import KafkaConsumer
import pandas as pd
import airflow
from airflow import DAG
from airflow.operators.python import PythonOperator
import datetime as dt
import time
from confluent_kafka import Consumer
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
import sqlalchemy 
from sqlalchemy.sql import text as sa_text


def _connect_to_source_database():
    # Create an MsSqlHook to connect to the target database
    hook_source = MsSqlHook(mssql_conn_id="sql_conn_source", schema="target")
    engine_source = hook_source.get_sqlalchemy_engine()
    return engine_source

def _connect_to_target_database():
    # Create an MsSqlHook to connect to the target database
    hook_target = MsSqlHook(mssql_conn_id="sql_conn_target", schema="target")
    engine_target = hook_target.get_sqlalchemy_engine()
    return engine_target



def _sync_source_and_target():
    # Connect to the source and target databases using MsSqlHook
    engine_source = _connect_to_source_database()
    engine_target = _connect_to_target_database()

    # Query data from the source table
    source_data = pd.read_sql("SELECT * FROM testDB.dbo.products", con=engine_source)

    # Truncate the target table to remove existing data
    engine_target.execute(sa_text('''
    truncate table dbo.products
    ''').execution_options(autocommit=True))

   

    # Insert data from the source table into the target table
    source_data.to_sql('products', engine_target, if_exists='append', index=False)

    # Print a confirmation message indicating successful synchronization
    print("Data has been successfully synchronized between source and target tables.")



dag = DAG( 
    dag_id="sync_source_target_SQL", 
    start_date=dt.datetime(2023, 9, 14), 
    schedule_interval=None, 
    tags=["kafka"]
)

sync_source_and_target = PythonOperator(
    task_id="sync_source_and_target",
    python_callable=_sync_source_and_target, 
    dag=dag,
)

sync_source_and_target

