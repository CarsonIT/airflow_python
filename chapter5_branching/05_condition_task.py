import airflow
import pendulum

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator

ERP_CHANGE_DATE = airflow.utils.dates.days_ago(1)


def _pick_erp_system(**context):
    if context["execution_date"] < airflow.utils.dates.days_ago(1):
        return "fetch_sales_old"
    else:
        return "fetch_sales_new"


def _deploy_model(**context):
    if _is_latest_run(**context):
        print("Deploying model")


def _is_latest_run(**context):
    # Lấy thời gian hiện tại ở múi giờ UTC
    now_utc = pendulum.now("UTC")

    # Đặt múi giờ cho Việt Nam (Asia/Ho_Chi_Minh)
    vietnam_timezone = pendulum.timezone("Asia/Ho_Chi_Minh")

    # Chuyển múi giờ của thời gian hiện tại sang múi giờ Việt Nam
    now_vietnam = now_utc.in_tz(vietnam_timezone)
    print("Now:", now_vietnam)

    left_window = context["dag"].following_schedule(context["execution_date"]).in_tz(vietnam_timezone)
    print("left_window:", left_window)

    right_window = context["dag"].following_schedule(left_window).in_tz(vietnam_timezone)
    print("right_window:", right_window)
    
    return left_window < now_vietnam <= right_window


with DAG(
    dag_id="05_condition_function",
    start_date=airflow.utils.dates.days_ago(3),
    schedule_interval="@daily",
    tags=["Branching"]
) as dag:
    start = DummyOperator(task_id="start")

    pick_erp = BranchPythonOperator(
        task_id="pick_erp_system", python_callable=_pick_erp_system
    )

    fetch_sales_old = DummyOperator(task_id="fetch_sales_old")
    clean_sales_old = DummyOperator(task_id="clean_sales_old")

    fetch_sales_new = DummyOperator(task_id="fetch_sales_new")
    clean_sales_new = DummyOperator(task_id="clean_sales_new")

    join_erp = DummyOperator(task_id="join_erp_branch", trigger_rule="none_failed")

    fetch_weather = DummyOperator(task_id="fetch_weather")
    clean_weather = DummyOperator(task_id="clean_weather")

    join_datasets = DummyOperator(task_id="join_datasets")
    train_model = DummyOperator(task_id="train_model")

    deploy_model = PythonOperator(task_id="deploy_model", python_callable=_deploy_model)

    start >> [pick_erp, fetch_weather]
    pick_erp >> [fetch_sales_old, fetch_sales_new]
    fetch_sales_old >> clean_sales_old
    fetch_sales_new >> clean_sales_new
    [clean_sales_old, clean_sales_new] >> join_erp
    fetch_weather >> clean_weather
    [join_erp, clean_weather] >> join_datasets
    join_datasets >> train_model >> deploy_model