from datetime import datetime, timedelta
from airflow.decorators import dag, task

default_args ={
    'owner': 'airflow',
    'depends_on_past': False,
    "start_date": datetime(2026,8,27),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "catchup": False,
    "schedule":"@daily"


}