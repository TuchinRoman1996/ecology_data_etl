from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.python import PythonOperator
from airflow.sensors.base import BaseSensorOperator
from airflow.utils.dates import days_ago
from airflow.utils.decorators import apply_defaults
from airflow.models import Connection


from psycopg2 import connect
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from psycopg2.extras import RealDictCursor
from psycopg2 import OperationalError

import sys
from os.path import abspath, dirname

current_dir = dirname(abspath(__file__))
parent_dir = dirname(current_dir)
sys.path.append(parent_dir)

from scripts.extract_from_postgres import extract_from_postgres_hist
from scripts.transform_data import transform_data_function_h
from scripts.load_to_postgres import load_to_postgres_hist_function


class PostgresNotificationSensor(BaseSensorOperator):
    @apply_defaults
    def __init__(self, conn_params, channel, *args, **kwargs):
        super(PostgresNotificationSensor, self).__init__(*args, **kwargs)
        self.conn_params = conn_params
        self.channel = channel
        self.nvos_id = None

    def poke(self, context):
        self.log.info(f"Начинается прослушка канала: {self.channel}")
        conn = connect(**self.conn_params)
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        cursor.execute(f"LISTEN {self.channel}")

        try:
            while True:
                conn.poll()
                while conn.notifies:
                    notify = conn.notifies.pop()
                    self.log.info(f"Получили значение из канала {notify.channel}: {notify.payload}")
                    if notify.channel == self.channel:
                        self.xcom_push(context, key='nvos_id', value=notify.payload)
                        return True
        except OperationalError as e:
            self.log.error(f"Ошибка прослушки: {str(e)}")
            return False
        finally:
            cursor.close()
            conn.close()


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

default_args = {
    'owner': 'Username',
    'depends_on_past': False,
    'start_date': days_ago(0),
    'catchup': 'false'
}

dag = DAG('Stream', default_args=default_args, schedule=None, schedule_interval='*/1 * * * *')
notification_sensor = PostgresNotificationSensor(
    task_id='check_postgresql',
    conn_params=source_db_params,
    channel='dag_trigger_channel',
    poke_interval=60,
    dag=dag,
)

extract_operator = PythonOperator(
    task_id='extract_data',
    python_callable=extract_from_postgres_hist,
    provide_context=True,
    dag=dag,
    op_args=[source_db_params]
)

transform_operator = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data_function_h,
    provide_context=True,
    op_args=[target_db_params],
    dag=dag,
)

load_operator = PythonOperator(
    task_id='load_data',
    python_callable=load_to_postgres_hist_function,
    provide_context=True,
    op_args=[target_db_params],
    dag=dag,
)

trigger_dag_operator = TriggerDagRunOperator(
    task_id='trigger_stream_dag',
    trigger_dag_id='Stream',
    dag=dag
)

notification_sensor >> extract_operator >> transform_operator >> load_operator >> trigger_dag_operator
