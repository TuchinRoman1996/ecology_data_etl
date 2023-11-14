from airflow import DAG
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'RTuchin',
    'depends_on_past': False,
    'start_date': days_ago(0),
    'catchup': 'false'
}

with DAG(default_args=default_args, )