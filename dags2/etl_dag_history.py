from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from airflow.models import Connection

import sys
from os.path import abspath, dirname

current_dir = dirname(abspath(__file__))
parent_dir = dirname(current_dir)
sys.path.append(parent_dir)

from scripts.extract_from_postgres import extract_from_postgres_hist
from scripts.transform_data import transform_data_function_h
from scripts.load_to_postgres import load_to_postgres_hist_function


default_args = {
    'owner': 'Username',
    'depends_on_past': False,
    'start_date': days_ago(0),
    'catchup': 'false'
}

source_db_conn = Connection.get_connection_from_secrets('source_db')
target_db_conn = Connection.get_connection_from_secrets('target_db')

source_db_params = {
    'database': source_db_conn.extra_dejson.get('database'),
    'user': source_db_conn.login,
    'password': source_db_conn.password,
    'host': source_db_conn.host,
    'port': source_db_conn.port,
}

target_db_params = {
            'database': target_db_conn.extra_dejson.get('database'),
            'user': target_db_conn.login,
            'password': target_db_conn.password,
            'host': target_db_conn.host,
            'port': target_db_conn.port,
}

dag = DAG(
    'History',
    default_args=default_args,
    schedule_interval='@once',
    schedule=None
)

extract_history_data = PythonOperator(
    task_id='extract_data_h',
    python_callable=extract_from_postgres_hist,
    provide_context=True,
    dag=dag,
    op_args=[source_db_params]
)

transform_data_h = PythonOperator(
    task_id='transform_data_h',
    python_callable=transform_data_function_h,
    provide_context=True,
    op_args=[target_db_params],
    dag=dag,
)

load_object_category_h = PythonOperator(
    task_id='load_data_h',
    python_callable=load_to_postgres_hist_function,
    provide_context=True,
    op_args=[target_db_params],
    dag=dag,
)

extract_history_data >> transform_data_h >> load_object_category_h
