from airflow import DAG
from airflow.models import Connection
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.dates import days_ago
from lxml import etree
import psycopg2
import datetime

target_db_conn = Connection.get_connection_from_secrets('test_db')
target_db_params = {
    'database': target_db_conn.extra_dejson.get('dbname'),
    'user': target_db_conn.login,
    'password': target_db_conn.password,
    'host': target_db_conn.host,
    'port': target_db_conn.port,
}


def load_set_metadata_to_stg(**kwargs):
    conn = psycopg2.connect(**target_db_params)
    cursor = conn.cursor()

    requests_id = int(datetime.datetime.now().strftime("%Y%m%d%H%M%S"))
    kwargs['ti'].xcom_push(key='request_id', value=requests_id)

    cursor.execute(f"""
        INSERT INTO stg."DWH/DSO/2STGmetadata"
        (request_id, status, load_date, table_name, filename)
        VALUES({requests_id}, 
                'CREATE', 
                '{datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")}', 
                '1STGemployees', 
                'employees_data_2023-10-27_165132.000.xml');
    """)

    conn.commit()
    conn.close()


def load_employees_to_stg(**kwargs):
    conn = psycopg2.connect(**target_db_params)
    cursor = conn.cursor()

    with open('/mnt/c/Users/HW1/Desktop/Hadoop/employees_data_2023-10-27_165132.000.xml', 'rb') as xml_file:
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

with DAG('load_employees_from_xml_to_stg', default_args=default_args, schedule_interval='@once', catchup=False) as dag:
    load_metadata_to_stg = PythonOperator(
        task_id='load_metadata_to_stg',
        python_callable=load_set_metadata_to_stg
    )

    load_employees_to_stg = PythonOperator(
        task_id='load_employees_to_stg',
        python_callable=load_employees_to_stg
    )

    load_employees_to_nds = PostgresOperator(
        task_id='load_employees_to_nds',
        sql="""
            CALL public.insert_into_tnds_employees();
            CALL public.insert_into_2ndsemployees();
        """,
        postgres_conn_id='test_db',
    )

    load_metadata_to_stg >> load_employees_to_stg >> load_employees_to_nds
