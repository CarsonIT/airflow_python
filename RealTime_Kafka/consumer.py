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


def _insert_data(after_json):
    after_list = []
    after_list.append(after_json)

    # Create a DataFrame from the data list
    df_new_data = pd.DataFrame(after_list)

    # Connect to the target database using MsSqlHook
    engine_target = _connect_to_target_database()

    # Write the new data to the 'products' table in the target database
    df_new_data.to_sql('products', engine_target, if_exists='append', index=False)
    
    # Print a confirmation message indicating successful data loading
    print("New data has been successfully loaded into the target database.")

def _delete_data(before_json):
    before_list = []
    before_list.append(before_json)

    # Create a DataFrame from the data list
    df_old_data = pd.DataFrame(before_list)

    # Connect to the target database using MsSqlHook
    engine_target = _connect_to_target_database()

    # Write the old data to a temporary table 'temp' in the target database (replace if exists)
    df_old_data.to_sql('temp', engine_target, if_exists='replace', index=False)
    
    # Execute a SQL query to delete old data from the 'products' table in the target database
    engine_target.execute(sa_text('''
    DELETE p
    FROM products AS p
    JOIN temp AS t ON p.id = t.id;
    ''').execution_options(autocommit=True))

    # Print a confirmation message indicating successful deletion of old data
    print("Old data has been successfully deleted in the target database.")

def _update_data(after_json):
    after_list = []
    after_list.append(after_json)

    # Create a DataFrame from the data list
    df_new_data = pd.DataFrame(after_list)

    # Connect to the target database using MsSqlHook
    engine_target = _connect_to_target_database()

    # Write the new data to a temporary table 'temp' in the target database (replace if exists)
    df_new_data.to_sql('temp', engine_target, if_exists='replace', index=False)
    
    # Execute a SQL query to update data in the 'products' table in the target database
    engine_target.execute(sa_text('''
    UPDATE products
    SET name = t.name, description = t.description, weight = t.weight
    FROM products AS p
    JOIN temp AS t ON p.id = t.id;
    ''').execution_options(autocommit=True))  

    # Print a confirmation message indicating successful data update
    print("New data has been successfully updated in the target database.")

def _start_kafka_consumer():
    # Load environment variables and configure logging
    load_dotenv(verbose=True)
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)
    print("Starting consumer", os.environ["BOOTSTRAP_SERVER"])

    # Get the Kafka bootstrap server address from environment variables
    bootstrap_servers = os.environ["BOOTSTRAP_SERVER"]
    print("Bootstrap server: " , bootstrap_servers)

    # Get the Kafka consumer group ID from environment variables
    group_id = os.environ["CONSUMER_GROUP"]
    print("group_id : " , group_id)

    # Configure Kafka consumer settings
    consumer_config = {
        'bootstrap_servers': os.environ["BOOTSTRAP_SERVER"],
        'auto_offset_reset': 'earliest',
        'enable_auto_commit': True,
        'group_id': os.environ["CONSUMER_GROUP"],
        #'value_deserializer': lambda x: json.loads(x.decode("utf-8"))
    }

    # Create a Kafka consumer instance
    consumer = KafkaConsumer(**consumer_config)

    # Subscribe to the Kafka topic defined in environment variables
    consumer.subscribe([os.environ["TOPICS_PEOPLE_BASIC_NAME"]])
    return consumer

def _consumer():
    # Start the Kafka consumer
    consumer = _start_kafka_consumer()

    # Iterate through Kafka messages
    for message in consumer:
        try:
            # Check if the message has a value
            if message.value is not None:
                # Parse JSON data from the message
                data = json.loads(message.value)
                decode_json = data.get('payload')
                before_json = decode_json.get('before')
                after_json = decode_json.get('after')

                # Check if both before_json and after_json are None
                if before_json is None and after_json is None:
                    continue
                # If before_json is None, it's an insert operation
                elif before_json is None:
                    _insert_data(after_json)
                # If after_json is None, it's a delete operation
                elif after_json is None:
                    _delete_data(before_json)
                # Otherwise, it's an update operation
                else:
                    _update_data(after_json)
            else:
                # If the message has no value, print a waiting message
                print("Waiting for messages...")
        except Exception as e:
            # Handle exceptions and log errors
            logger.error(e)

dag = DAG( 
    dag_id="product_consumer_1", 
    start_date=dt.datetime(2023, 9, 14), 
    schedule_interval=None, 
    tags=["kafka"]
)

consumer = PythonOperator(
    task_id="consumer",
    python_callable=_consumer, 
    dag=dag,
)

sync_source_and_target = PythonOperator(
    task_id="sync_source_and_target",
    python_callable=_sync_source_and_target, 
    dag=dag,
)

sync_source_and_target >> consumer

