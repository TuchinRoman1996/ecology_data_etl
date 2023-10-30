from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
import time

default_args = {
    'owner': 'Username',
    'depends_on_past': False,
    'start_date': days_ago(0),
    'catchup': 'false'
}

dag = DAG(
    'Dummy_DAG',
    default_args=default_args,
    schedule_interval='@once',
    schedule=None,
    concurrency=4,
)


def DummyFunction():
    pass


def SleepingFunction():
    time.sleep(10)


task1 = PythonOperator(
    task_id='task1',
    python_callable=DummyFunction,
    provide_context=True,
    dag=dag,
)

task2 = PythonOperator(
    task_id='task2',
    python_callable=SleepingFunction,
    provide_context=True,
    dag=dag,
)

task3 = PythonOperator(
    task_id='task3',
    python_callable=DummyFunction,
    provide_context=True,
    dag=dag,
)

task4 = PythonOperator(
    task_id='task4',
    python_callable=SleepingFunction,
    provide_context=True,
    dag=dag,
)

task5 = PythonOperator(
    task_id='task5',
    python_callable=DummyFunction,
    provide_context=True,
    dag=dag,
)

task6 = PythonOperator(
    task_id='task6',
    python_callable=DummyFunction,
    provide_context=True,
    dag=dag,
)

task7 = PythonOperator(
    task_id='task7',
    python_callable=DummyFunction,
    provide_context=True,
    dag=dag,
)

task8 = PythonOperator(
    task_id='task8',
    python_callable=DummyFunction,
    provide_context=True,
    dag=dag,
)

task9 = PythonOperator(
    task_id='task9',
    python_callable=DummyFunction,
    provide_context=True,
    dag=dag,
)

task1 >> task2
task1 >> task3
task3 >> task4
task3 >> task5
task5 >> task6
task2 >> task6
task6 >> task7
task6 >> task8
task4 >> task9
