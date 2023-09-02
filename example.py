import datetime
from airflow import DAG
import pandas as pd
from airflow.sensors.python import PythonSensor
from airflow.operators.python_operator import PythonOperator
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
import sqlalchemy  
from sqlalchemy.sql import text as sa_text
import os


dag = DAG(
    dag_id="Example_Demo1",
    start_date=datetime.datetime(2023, 8, 17),
    catchup=False,
    tags=["example"],
    schedule_interval=None,
)

def truncate_table():
    try:
        #Tạo thông tin connect tới Database 
        hook_target = MsSqlHook(mssql_conn_id="connect_mssql", schema="YoutubeVideo")
        engine_target = hook_target.get_sqlalchemy_engine()
        #Truncate table 
        engine_target.execute(sa_text('''TRUNCATE TABLE Video''').execution_options(autocommit=True))
    except Exception as e:
        raise ValueError('Email to DataEngineer about error that: ',e)
        return False

def extract_CAvideos_load_db():
    try:
        # Read file CSV
        csv_file_path = '/home/ubuntu/Documents/CAvideos.csv'
        df = pd.read_csv(csv_file_path)

        # Chọn các cột cần thiết
        columns_to_select = ['video_id', 'trending_date', 'channel_title','category_id','publish_time','views','likes','dislikes','comment_count']
        # Tạo DataFrame mới và gán các cột cần thiết
        df_new = df[columns_to_select]

        # Thêm cột country_id 
        df_new['country_id'] = 5
        # Chuyển đổi cột 'trending_date' sang kiểu dữ liệu date
        df_new['trending_date'] = pd.to_datetime(df_new['trending_date'], format='%y.%d.%m')
        # Chuyển đổi cột 'publish_time' sang kiểu dữ liệu datetime
        df_new['publish_time'] = pd.to_datetime(df_new['publish_time'])


        #Tạo thông tin connect tới Database 
        hook_target = MsSqlHook(mssql_conn_id="connect_mssql", schema="YoutubeVideo")
        engine_target = hook_target.get_sqlalchemy_engine()

        df_new.to_sql('Video', engine_target, if_exists='append', index=False,
        dtype = {
       'video_id':  sqlalchemy.types.VARCHAR(length=11),
       'trending_date': sqlalchemy.types.DATE(),
       'channel_title':  sqlalchemy.types.NVARCHAR(length=100),
       'category_id': sqlalchemy.types.INTEGER(),
       'publish_time': sqlalchemy.types.DATETIME(),
       'views': sqlalchemy.types.INTEGER(),
       'likes': sqlalchemy.types.INTEGER(),
       'dislikes': sqlalchemy.types.INTEGER(),
       'comment_count': sqlalchemy.types.INTEGER(),
       'country_id': sqlalchemy.types.INTEGER()})

        print("Data load into target database successfully")
    except Exception as e:
        raise ValueError('Email to DataEngineer about error that: ',e)
        return False

def extract_DEvideos_load_db():
    try:
        # Read file CSV
        csv_file_path = '/home/ubuntu/Documents/DEvideos.csv'
        df = pd.read_csv(csv_file_path)

        # Chọn các cột cần thiết
        columns_to_select = ['video_id', 'trending_date', 'channel_title','category_id','publish_time','views','likes','dislikes','comment_count']
        # Tạo DataFrame mới và gán các cột cần thiết
        df_new = df[columns_to_select]

        # Thêm cột country_id tương ứng
        df_new['country_id'] = 4
        # Chuyển đổi cột 'trending_date' sang kiểu dữ liệu date
        df_new['trending_date'] = pd.to_datetime(df_new['trending_date'], format='%y.%d.%m')
        # Chuyển đổi cột 'publish_time' sang kiểu dữ liệu datetime
        df_new['publish_time'] = pd.to_datetime(df_new['publish_time'])

        print("**************************")
        df_new.count()
        print("**************************")
        #Tạo thông tin connect tới Database 
        hook_target = MsSqlHook(mssql_conn_id="connect_mssql", schema="YoutubeVideo")
        engine_target = hook_target.get_sqlalchemy_engine()

        df_new.to_sql('Video', engine_target, if_exists='append', index=False,
        dtype = {
       'video_id':  sqlalchemy.types.VARCHAR(length=11),
       'trending_date': sqlalchemy.types.DATE(),
       'channel_title':  sqlalchemy.types.NVARCHAR(length=100),
       'category_id': sqlalchemy.types.INTEGER(),
       'publish_time': sqlalchemy.types.DATETIME(),
       'views': sqlalchemy.types.INTEGER(),
       'likes': sqlalchemy.types.INTEGER(),
       'dislikes': sqlalchemy.types.INTEGER(),
       'comment_count': sqlalchemy.types.INTEGER(),
       'country_id': sqlalchemy.types.INTEGER()})

        print("Data load into target database successfully")
    except Exception as e:
        raise ValueError('Email to DataEngineer about error that: ',e)
        return False


def extract_FRvideos_load_db():
    try:
        # Read file CSV
        csv_file_path = '/home/ubuntu/Documents/FRvideos.csv'
        df = pd.read_csv(csv_file_path)

        # Chọn các cột cần thiết
        columns_to_select = ['video_id', 'trending_date', 'channel_title','category_id','publish_time','views','likes','dislikes','comment_count']
        # Tạo DataFrame mới và gán các cột cần thiết
        df_new = df[columns_to_select]

        # Thêm cột country_id tương ứng
        df_new['country_id'] = 4
        # Chuyển đổi cột 'trending_date' sang kiểu dữ liệu date
        df_new['trending_date'] = pd.to_datetime(df_new['trending_date'], format='%y.%d.%m')
        # Chuyển đổi cột 'publish_time' sang kiểu dữ liệu datetime
        df_new['publish_time'] = pd.to_datetime(df_new['publish_time'])

        print("**************************")
        df_new.count()
        print("**************************")
        #Tạo thông tin connect tới Database 
        hook_target = MsSqlHook(mssql_conn_id="connect_mssql", schema="YoutubeVideo")
        engine_target = hook_target.get_sqlalchemy_engine()

        df_new.to_sql('Video', engine_target, if_exists='append', index=False,
        dtype = {
       'video_id':  sqlalchemy.types.VARCHAR(length=11),
       'trending_date': sqlalchemy.types.DATE(),
       'channel_title':  sqlalchemy.types.NVARCHAR(length=100),
       'category_id': sqlalchemy.types.INTEGER(),
       'publish_time': sqlalchemy.types.DATETIME(),
       'views': sqlalchemy.types.INTEGER(),
       'likes': sqlalchemy.types.INTEGER(),
       'dislikes': sqlalchemy.types.INTEGER(),
       'comment_count': sqlalchemy.types.INTEGER(),
       'country_id': sqlalchemy.types.INTEGER()})

        print("Data load into target database successfully")
    except Exception as e:
        raise ValueError('Email to DataEngineer about error that: ',e)
        return False
# def extract_csv_load_db():
#     try:
#         # Đọc file CSV
#         csv_file_path = '/home/ubuntu/Documents/example_companies.csv'
#         df = pd.read_csv(csv_file_path)
#         print(df)
#         hook_target = MsSqlHook(mssql_conn_id="conn", schema="test")
#         engine_target = hook_target.get_sqlalchemy_engine()
#         df.to_sql('temp', engine_target, if_exists='replace', index=False)
#         print("Data load into target database successfully")
#     except Exception as e:
#         print("Data extract error: " + str(e))

def extract_json_load_db():
    try:
        os.chdir('/home/ubuntu/Documents')
        csv_files = [f for f in os.listdir() if f.endswith('.csv')]
        dfs = []
        for csv in csv_files:
            df = pd.read_csv(csv)
            dfs.append(df)
        final_df = pd.concat(dfs, ignore_index=True)
        print("Data load into target database successfully")
        final_df.count()
        print("Data load into target database successfully")
        # hook_target = MsSqlHook(mssql_conn_id="conn", schema="test")
        # engine_target = hook_target.get_sqlalchemy_engine()
        # df.to_sql('JSON_TEST', engine_target, if_exists='replace', index=False)
    except Exception as e:
        raise ValueError('Email to DataEngineer about error that: ',e)
        return False




extract_CAvideos_load_db = PythonOperator(
    task_id="extract_CAvideos_load_db",
    python_callable=extract_CAvideos_load_db,
    dag=dag,
    )

extract_DEvideos_load_db = PythonOperator(
    task_id="extract_DEvideos_load_db",
    python_callable=extract_DEvideos_load_db,
    dag=dag,
    )


extract_FRvideos_load_db = PythonOperator(
    task_id="extract_FRvideos_load_db",
    python_callable=extract_FRvideos_load_db,
    dag=dag,
    )

extract_json_load_db = PythonOperator(
    task_id="extract_json_load_db",
    python_callable=extract_json_load_db,
    dag=dag,
    )


truncate_table = PythonOperator(
    task_id="truncate_table",
    python_callable=truncate_table,
    dag=dag,
    )

truncate_table >> [extract_CAvideos_load_db,extract_DEvideos_load_db,extract_FRvideos_load_db] >> extract_json_load_db