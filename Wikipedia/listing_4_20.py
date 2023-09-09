from urllib import request
import os 
import airflow.utils.dates
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.microsoft.mssql.operators.mssql import MsSqlOperator

# default_args = {
#     'email': ['carsonit01@gmail.com'],
#     'email_on_failure': True,
# }

dag = DAG(
    dag_id="listing_4_20_1",
    start_date=airflow.utils.dates.days_ago(1),
    schedule_interval="@hourly",
    max_active_runs=1,
    default_args = default_args,
    tags=["Wiki"]
)


def _get_data(year, month, day, hour, output_path, **_):
    url = (
        "https://dumps.wikimedia.org/other/pageviews/"
        f"{year}/{year}-{month:0>2}/pageviews-{year}{month:0>2}{day:0>2}-{hour:0>2}0000.gz"
    )
    request.urlretrieve(url, output_path)


get_data = PythonOperator(
    task_id="get_data",
    python_callable=_get_data,
    op_kwargs={
        "year": "{{ execution_date.year }}",
        "month": "{{ execution_date.month }}",
        "day": "{{ execution_date.day }}",
        "hour": "{{ execution_date.hour }}",
        "output_path": "/home/ubuntu/Downloads/wiki/wikipageviews_test002.gz",
    },
    dag=dag,
)

extract_gz = BashOperator(
    task_id="extract_gz", 
    bash_command="gunzip --force /home/ubuntu/Downloads/wiki/wikipageviews_test002.gz", 
    dag=dag
)


def _fetch_pageviews(pagenames, execution_date):
    result = dict.fromkeys(pagenames, 0)
    with open("/home/ubuntu/Downloads/wiki/wikipageviews_test002", "r") as f:
        for line in f:
            domain_code, page_title, view_counts, _ = line.split(" ")
            if domain_code == "en" and page_title in pagenames:
                result[page_title] = view_counts

    with open("/home/ubuntu/Downloads/wiki/sql_server_query.sql", "w") as f:
        for pagename, pageviewcount in result.items():
            f.write(
                "INSERT INTO pageview_counts VALUES ("
                f"'{pagename}', {pageviewcount}, '{execution_date}'"
                ");\n"
            )
    # Prints e.g. "{'Facebook': '778', 'Apple': '20', 'Google': '451', 'Amazon': '9', 'Microsoft': '119'}"


fetch_pageviews = PythonOperator(
    task_id="fetch_pageviews",
    python_callable=_fetch_pageviews,
    op_kwargs={"pagenames": {"Google", "Amazon", "Apple", "Microsoft", "Facebook"}},
    dag=dag,
)

sql_file_path = '/home/ubuntu/Downloads/wiki/sql_server_query.sql'
# Kiểm tra xem tệp SQL tồn tại
if os.path.exists(sql_file_path):
    with open(sql_file_path, "r") as sql_file:
        sql_server_query = sql_file.read()
else:
    sql_server_query =""
# with open("/home/ubuntu/Downloads/wiki/sql_server_query.sql", "r") as sql_file:
#     sql_server_query = sql_file.read()

write_to_sql_server = MsSqlOperator(
    task_id="write_to_sql_server",
    mssql_conn_id="connect_sql_wiki",
    sql=sql_server_query,
    dag=dag,
    )

get_data >> extract_gz >> fetch_pageviews >> write_to_sql_server