import time
from airflow import DAG
from airflow.models import Connection
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.dates import days_ago
from airflow.models.baseoperator import BaseOperator
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowException
from lxml import etree
import subprocess
import datetime
import psycopg2

target_db_conn = Connection.get_connection_from_secrets('test_db')
target_db_params = {
    'database': target_db_conn.extra_dejson.get('dbname'),
    'user': target_db_conn.login,
    'password': target_db_conn.password,
    'host': target_db_conn.host,
    'port': target_db_conn.port,
}


class MyCustomSensor(BaseOperator):
    def __init__(self, db_params, interval, *args, **kwargs):
        super(MyCustomSensor, self).__init__(*args, **kwargs)
        self.db_params = db_params
        self.interval = interval

    def execute(self, context):
        conn = psycopg2.connect(**self.db_params)
        cursor = conn.cursor()
        try:
            while True:
                # Получаем список уже загруженных файлов из таблицы метаданных в STG
                cursor.execute('select filename from stg."DWH/DSO/2STGmetadata";')
                db_files = [record[0] for record in cursor.fetchall()]
                print(f'Файлы, которые уже загруженны в БД {db_files}')

                # Отправляю запрос на получение списка файлов
                current_file = subprocess.check_output('ls /mnt/c/Users/HW1/Desktop/Hadoop/', shell=True)
                current_file = current_file.decode('utf-8').split('\n')[:-1]
                print(f'Файлы в дериктории: {current_file}')

                new_files = [file for file in current_file if file not in db_files]
                print(f'Новые файлы {new_files}')

                if new_files:
                    print(f'Обнаружен новый файл: {new_files}')
                    context['ti'].xcom_push(key='filename', value=new_files)
                    cursor.close()
                    conn.close()
                    return True
                else:
                    print(f"Новых файлов не обнаружено. Повторная попытка через {self.interval} секунд")
                    time.sleep(self.interval)
        except AirflowException:
            cursor.close()
            conn.close()
            return False


def load_set_metadata_to_stg(db_params, **kwargs):
    conn = psycopg2.connect(**db_params)
    cursor = conn.cursor()

    filename = kwargs['ti'].xcom_pull(task_ids='file_sensor', key='filename')

    for i in filename:
        requests_id = int(datetime.datetime.now().strftime("%Y%m%d%H%M%S"))
        kwargs['ti'].xcom_push(key='request_id', value=requests_id)

        cursor.execute(f"""
            INSERT INTO stg."DWH/DSO/2STGmetadata"
            (request_id, status, load_date, table_name, filename)
            VALUES({requests_id}, 
                'CREATE', 
                '{datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")}', 
                '1STGemployees',
                '{i}');
        """)

    conn.commit()
    conn.close()


def load_employees_to_stg(db_params, **kwargs):
    conn = psycopg2.connect(**db_params)
    cursor = conn.cursor()

    filename = kwargs['ti'].xcom_pull(task_ids='file_sensor', key='filename')

    for i in filename:
        with open(f'/mnt/c/Users/HW1/Desktop/Hadoop/{i}', 'rb') as xml_file:
            parser = etree.iterparse(xml_file, events=('end',))

            ti = kwargs['ti']
            request_id = ti.xcom_pull(task_ids='load_metadata_to_stg', key='request_id')

            for event, element in parser:
                if element.tag == 'record':
                    cursor.execute(f"""
                        INSERT INTO stg."DWH/DSO/1STGemployees" (request_id, id_employee, fio, 
                        id_region, id_company, l_faktor, j_faktor, x_faktor, birth_date, date_devations, uik, 
                        uik_num) VALUES (
                            {request_id},
                            '{element.findtext('IDсотрудника')}',
                            '{element.findtext('ФИО')}',
                            '{element.findtext('IDРегиона')}', 
                            '{element.findtext('IDКомпании')}', 
                            '{element.findtext('Лфактор')}', 
                            '{element.findtext('Жфактор')}', 
                            '{element.findtext('Хфактор')}', 
                            '{element.findtext('Датарождения')}', 
                            '{element.findtext('Датаотклонения')}',
                            '{element.findtext('УИК')}',
                            '{element.findtext('УИКЧисло')}'
                        );
                    """)

        conn.commit()
        conn.close()


default_args = {
    'owner': 'Username',
    'depends_on_past': False,
    'start_date': days_ago(0),
    'catchup': 'false'
}

with DAG('load_employees_from_xml_to_stg_stream', default_args=default_args, schedule_interval="@once",
         catchup=False) as dag:
    t1 = MyCustomSensor(
        task_id='file_sensor',
        db_params=target_db_params,
        interval=30,
    )

    t2 = PythonOperator(
        task_id='load_metadata_to_stg',
        python_callable=load_set_metadata_to_stg,
        op_args=[target_db_params]
    )

    t3 = PythonOperator(
        task_id='load_employees_to_stg',
        python_callable=load_employees_to_stg,
        op_args=[target_db_params]

    )

    t4 = PostgresOperator(
        task_id='load_employees_to_nds',
        sql="""
            select nds.insert_delta_into_nds_employees();
            select nds.insert_nds_employees();
           """,
        postgres_conn_id='test_db',
    )

    t5 = TriggerDagRunOperator(
        task_id='trigger_stream_dag',
        trigger_dag_id='load_employees_from_xml_to_stg_stream',
        dag=dag
    )

    t1 >> t2 >> t3 >> t4 >> t5
