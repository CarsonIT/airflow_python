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
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
import sqlalchemy 
from sqlalchemy.sql import text as sa_text
from sqlalchemy import create_engine


def _connect_to_source_database():
    # Create an MsSqlHook to connect to the target database
    hook_source = MsSqlHook(mssql_conn_id="sql_conn_source", schema="target")
    engine_source = hook_source.get_sqlalchemy_engine()
    return engine_source

def _connect_to_database_postgres():
    pg_hook = PostgresHook(postgres_conn_id='postgres_conn', schema='postgres')
    connection_uri = pg_hook.get_uri()
    engine_postgres = create_engine(connection_uri)
    return engine_postgres

# def _insert_dataframe_to_postgres():
#     # Connect to the source and target databases using MsSqlHook
#     engine_source = _connect_to_source_database()
#     # Query data from the source table
#     source_data = pd.read_sql("SELECT * FROM testDB.dbo.products", con=engine_source)

#     engine_postgres = _connect_to_database_postgres()
#     source_data.to_sql('products', engine_postgres, if_exists='append', chunksize=1000, index=False)
#     # Print a confirmation message indicating successful synchronization
#     print("Data has been successfully synchronized between source and target tables.")

def _sync_source_and_target():
    # Connect to the source and target databases using MsSqlHook
    engine_source = _connect_to_source_database()
    engine_target = _connect_to_database_postgres()

    # Query data from the source table
    source_data = pd.read_sql("SELECT * FROM testDB.dbo.products", con=engine_source)

    # Truncate the target table to remove existing data
    # Mở kết nối đến cơ sở dữ liệu PostgreSQL
    connection = engine_target.connect()
    # Thực hiện câu lệnh SQL TRUNCATE và commit
    sql = "TRUNCATE TABLE products;"
    connection.execute(sql)
    connection.close()
    # Insert data from the source table into the target table
    source_data.to_sql('products', engine_target, if_exists='append', chunksize=1000, index=False)
    # Print a confirmation message indicating successful synchronization
    print("Data has been successfully synchronized between source and target tables.")

# def _truncate_table():
#     pg_hook = PostgresHook(postgres_conn_id='postgres_conn', schema='postgres')
#     connection_uri = pg_hook.get_uri()
#     engine = create_engine(connection_uri)
#     # Mở kết nối đến cơ sở dữ liệu PostgreSQL
#     connection = engine.connect()
    
#     # Thực hiện câu lệnh SQL TRUNCATE và commit
#     sql = "TRUNCATE TABLE products;"
#     connection.execute(sql)
#     connection.close()


dag = DAG( 
    dag_id="sync_source_SQL_target_Postgres", 
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

