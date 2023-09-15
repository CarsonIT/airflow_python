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


def _insert_data(after_json):
    after_list = []
    after_list.append(after_json)
    df_new_data = pd.DataFrame(after_list)
    # Tạo thông tin connect tới Database 
    hook_target = MsSqlHook(mssql_conn_id="sql_conn", schema="target")
    engine_target = hook_target.get_sqlalchemy_engine()
    # Insert_data
    df_new_data.to_sql('products', engine_target, if_exists='append', index=False)
    print("New data load into target database successfully")

def insert_data(after_json):

                    kafka_message = f"""
                    Message before: {before_json}
                    Message after: {after_json}
                    Message key: {message.key}
                    Message partition: {message.partition}
                    Message offset: {message.offset}
                    """
                    logger.info(kafka_message)

                    before_list = []
                    before_list.append(before_json)
                    df_temp = pd.DataFrame(before_list)
                    print("Message before df: /n")
                    print(df_temp)
                    # Tạo thông tin connect tới Database 
                    hook_target = MsSqlHook(mssql_conn_id="sql_conn", schema="target")
                    engine_target = hook_target.get_sqlalchemy_engine()

                    df_temp.to_sql('temp', engine_target, if_exists='replace', index=False)
                    engine_target.execute(sa_text('''
                    DELETE p
                    FROM products AS p
                    JOIN temp AS t ON p.id = t.id;
                    ''').execution_options(autocommit=True))

                    print("Xu ly Delete thanh cong")

def Consumer():
    load_dotenv(verbose=True)
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)
    print("Starting consumer", os.environ["BOOTSTRAP_SERVER"])

    bootstrap_servers = os.environ["BOOTSTRAP_SERVER"]
    print("Bootstrap server: " , bootstrap_servers)
    group_id = os.environ["CONSUMER_GROUP"]
    print("group_id : " , group_id)

    consumer_config = {
        'bootstrap_servers': os.environ["BOOTSTRAP_SERVER"],
        'auto_offset_reset': 'earliest',
        'enable_auto_commit': True,
        'group_id': os.environ["CONSUMER_GROUP"],
        #'value_deserializer': lambda x: json.loads(x.decode("utf-8"))
    }

    # Create a Kafka consumer instance
    consumer = KafkaConsumer(**consumer_config)

    consumer.subscribe([os.environ["TOPICS_PEOPLE_BASIC_NAME"]])

    
    for message in consumer:
        try:
            if message.value is not None:
                #Read data from comsumer
                data = json.loads(message.value)
                #take change data
                decode_json = data.get('payload')
                #take before and after   
                before_json = decode_json.get('before')
                after_json = decode_json.get('after')
                if before_json is None and after_json is None:
                    continue
                elif before_json is None:
                    _insert_data(after_json)
                elif after_json is None:
                    

                else:
                    after_list = []
                    after_list.append(after_json)

                    df_temp = pd.DataFrame(after_list)
                    # Tạo thông tin connect tới Database 
                    hook_target = MsSqlHook(mssql_conn_id="sql_conn", schema="target")
                    engine_target = hook_target.get_sqlalchemy_engine()

                    df_temp.to_sql('temp', engine_target, if_exists='replace', index=False)
                    engine_target.execute(sa_text('''
                    UPDATE products
                    SET name = t.name, description = t.description, weight = t.weight
                    FROM products AS p
                    JOIN temp AS t ON p.id = t.id;
                    ''').execution_options(autocommit=True))
                
                    print("Xu ly update lay After")

                # kafka_message = f"""
                # Message received: {message.value}
                # Message before: {before_json}
                # Message after: {after_json}
                # Message key: {message.key}
                # Message partition: {message.partition}
                # Message offset: {message.offset}
                # """
                # logger.info(kafka_message)
            else:
                print("Wait message")

        except Exception as e:
            logger.error(e)
  
dag = DAG( 
    dag_id="test_consumer", 
    start_date=dt.datetime(2023, 9, 14), 
    schedule_interval=None, 
    tags=["kafka"]
)

Consumer = PythonOperator(
    task_id="Consumer",
    python_callable=Consumer, 
    dag=dag,
)

Consumer

