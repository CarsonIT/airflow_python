from urllib import request

import airflow.utils.dates
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.microsoft.mssql.operators.mssql import MsSqlOperator

dag = DAG(
    dag_id="test_sql",
    start_date=airflow.utils.dates.days_ago(1),
    schedule_interval="@hourly",
    max_active_runs=1,
    tags=["Wiki"]
)

# Đọc nội dung của tệp SQL
with open("/home/ubuntu/airflow-environment/dags/scripts/sql_server_query.sql", "r") as sql_file:
    sql_content = sql_file.read()


write_to_sql_server = MsSqlOperator(
    task_id="write_to_sql_server",
    mssql_conn_id="connect_sql_wiki",
    #sql="sql_server_query.sql ",
    sql=sql_content,
    # sql=r"""INSERT INTO [test].[dbo].[pageview_counts] VALUES ('Microsoft', 178, '2023-09-08T12:00:00+00:00');
    #         INSERT INTO [test].[dbo].[pageview_counts] VALUES ('Apple', 83, '2023-09-08T12:00:00+00:00');""",
    dag=dag,
    )


write_to_sql_server