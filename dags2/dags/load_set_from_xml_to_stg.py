import datetime
from airflow import DAG
from airflow.models import Connection
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.dates import days_ago
from lxml import etree
import psycopg2


target_db_conn = Connection.get_connection_from_secrets('test_db')
target_db_params = {
    'database': target_db_conn.extra_dejson.get('dbname'),
    'user': target_db_conn.login,
    'password': target_db_conn.password,
    'host': target_db_conn.host,
    'port': target_db_conn.port,
}


def get_metadata(**kwargs):
    kwargs['ti'].xcom_push(key='load_date', value=datetime.datetime.now())


def load_set_company_to_stg(**kwargs):

    conn = psycopg2.connect(**target_db_params)
    cursor = conn.cursor()

    request_id = int(datetime.datetime.now().strftime("%Y%m%d%H%M%S")) + 1
    load_date = kwargs['ti'].xcom_pull(key='load_date')

    cursor.execute(f"""
    INSERT INTO stg."DWH/DSO/2STGmetadata" (request_id, status, load_date, table_name, filename)
    VALUES ({request_id}, 'CREATE', '{load_date}', '"DWH/AO/0companies"', 'companies_data_2023-10-27_164957.912.xml')
    """)

    conn.commit()

    with open('/mnt/c/Users/HW1/Desktop/Hadoop/companies_data_2023-10-27_164957.912.xml', 'rb') as xml_file:
        parser = etree.iterparse(xml_file, events=('end',))

        for event, element in parser:
            if element.tag == 'record':
                cursor.execute(f"""INSERT INTO stg."DWH/AO/0companies" (request_id, id_company, company_parent, 
                id_region, type_company, type_industry, rosstat, create_date, date2, inn) VALUES( 
                        '{request_id}',
                        '{element.findtext('IDКомпании')}', 
                        '{element.findtext('ГоловнаяОрганизация')}',
                        '{element.findtext('idРегиона')}', 
                        '{element.findtext('ТипКомпании')}', 
                        '{element.findtext('ТипОтрасли')}', 
                        '{element.findtext('Росстат')}', 
                        '{element.findtext('Датаcозданияорг')}', 
                        '{element.findtext('Дата2')}', 
                        '{element.findtext('ИНН')}');""")
                element.clear()
    conn.commit()
    conn.close()


def load_set_regions_to_stg(**kwargs):
    conn = psycopg2.connect(**target_db_params)
    cursor = conn.cursor()

    request_id = int(datetime.datetime.now().strftime("%Y%m%d%H%M%S")) + 2
    load_date = kwargs['ti'].xcom_pull(key='load_date')

    cursor.execute(f"""
    INSERT INTO stg."DWH/DSO/2STGmetadata" (request_id, status, load_date, table_name, filename)
    VALUES ({request_id}, 'CREATE', '{load_date}', '"DWH/AO/0regions"', 'regions_data_2023-10-27_164957.912.xml')
    """)
    conn.commit()

    with open('/mnt/c/Users/HW1/Desktop/Hadoop/regions_data_2023-10-27_164957.912.xml', 'rb') as xml_file:
        parser = etree.iterparse(xml_file, events=('end',))

        for event, element in parser:
            if element.tag == 'record':
                cursor.execute(f"""INSERT INTO stg."DWH/AO/0regions" (request_id, region_name, id_region, region_parent, 
                population, name_eng, code, region_oktmo, region_okato, f_value) VALUES( 
                        '{request_id}',
                        '{element.findtext('НазваниеРегиона')}',
                        '{element.findtext('IDРегиона')}',
                        '{element.findtext('ФО')}', 
                        '{element.findtext('ГражданКол-во')}', 
                        '{element.findtext('Name')}', 
                        '{element.findtext('Code')}', 
                        '{element.findtext('ОКТМО')}', 
                        '{element.findtext('ОКАТО')}', 
                        '{element.findtext('Ф_Значение')}');""")
                element.clear()
    conn.commit()
    conn.close()


default_args = {
    'owner': 'Username',
    'depends_on_past': False,
    'start_date': days_ago(0),
    'catchup': 'false'
}

with DAG('load_set_from_xml_to_stg', default_args=default_args, schedule_interval='@once', catchup=False) as dag:
    get_metadata = PythonOperator(
        task_id='get_metadata',
        python_callable=get_metadata
    )

    load_company_to_stg = PythonOperator(
        task_id='load_company_to_stg',
        python_callable=load_set_company_to_stg
    )

    load_regions_to_stg = PythonOperator(
        task_id='load_regions_to_stg',
        python_callable=load_set_regions_to_stg
    )

    load_regions_to_nds = PostgresOperator(
        task_id='load_regions_to_nds',
        postgres_conn_id='test_db',
        sql='call nds.insert_into_hnds_regions();'
    )

    load_company_to_nds = PostgresOperator(
        task_id='load_company_to_nds',
        postgres_conn_id='test_db',
        sql='call nds.insert_into_hnds_company();',
    )

    get_metadata >> [load_company_to_stg, load_regions_to_stg] >> load_regions_to_nds >> load_company_to_nds
