from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.ftp.hooks.ftp import FTPHook
from airflow.sensors.base import BaseSensorOperator
from airflow.utils.dates import days_ago
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.exceptions import AirflowException
from lxml import etree
import uuid
from io import BytesIO

import cProfile

profiler = cProfile.Profile()

default_args = {
    'owner': 'RTuchin',
    'depends_on_past': False,
    'start_date': days_ago(0),
    'catchup': 'false'
}


def update_metadata_status(status, request_id, pg_hook):
    try:
        sql = f"""
            UPDATE stg."DWH_DSO_2STGmetadata"
            SET status = '{status}'
            WHERE request_id = '{request_id}'
            and status != 'FAILED';
        """
        pg_hook.run(sql)
    except Exception as e:
        print(f"Failed to update metadata status: {str(e)}")


def get_filename_list(**kwargs):
    db_hook = PostgresHook(postgres_conn_id='test_db')

    sql = """
        SELECT filename
        FROM stg."DWH_DSO_2STGmetadata";
    """
    results = db_hook.get_records(sql)
    if results:
        filenames = set(result[0] for result in results)
    else:
        filenames = set()

    print(f'Список файлов в БД:\n{filenames}')
    kwargs['ti'].xcom_push(key='filename_list', value=filenames)


class CustomFTPSensor(BaseSensorOperator):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.ftp_hook = None
        self.uploaded_files = None
        self.ftp_files = None

    def poke(self, context):
        self.ftp_hook = FTPHook(ftp_conn_id='ftp_chtd')
        self.uploaded_files = context['ti'].xcom_pull(key='filename_list')

        try:
            ftp_files = set(self.ftp_hook.list_directory("for_chtd/test_kxd_glavnivc"))
            print(f'Список файлов в FTP:\n{ftp_files}')
            new_files = ftp_files - self.uploaded_files

            if new_files:
                for file in new_files:
                    if 'employee' in str(file):
                        print(f'Новый файл employee: {file}')
                        context['ti'].xcom_push(key='employee_filename', value=file)
                        return True
                    elif 'company' in str(file):
                        print(f'Новый файл company: {file}')
                    elif 'region' in str(file):
                        print(f'Новый файл region: {file}')
                    else:
                        return False
            else:
                return False

        except AirflowException as e:
            raise AirflowException(f"Ошибка сенсора FTP: {str(e)}")
        finally:
            self.ftp_hook.close_conn()


def load_metadata_to_staging(**kwargs):
    request_id = str(uuid.uuid4())
    table_name = 'DWH_DSO_2STGmetadata'
    filename = kwargs['ti'].xcom_pull(key='employee_filename')
    pg_hook = PostgresHook(postgres_conn_id='test_db')

    sql = f"""
        INSERT INTO stg."DWH_DSO_2STGmetadata"(request_id, table_name, filename)
        VALUES('{request_id}', '{table_name}', '{filename}');
    """
    kwargs['ti'].xcom_push(key='request_id', value=request_id)
    pg_hook.run(sql)


profiler.enable()


def process_large_xml_and_insert_to_db(ftp_conn_id, postgres_conn_id, batch_size, **kwargs):
    ftp_hook = FTPHook(ftp_conn_id)
    pg_hook = PostgresHook(postgres_conn_id)
    filename = kwargs['ti'].xcom_pull(key='employee_filename')
    request_id = kwargs['ti'].xcom_pull(key='request_id')

    with BytesIO() as xml_buffer:
        ftp_hook.retrieve_file(f'for_chtd/test_kxd_glavnivc/{filename}', xml_buffer)
        xml_buffer.seek(0)

        context = etree.iterparse(xml_buffer, events=('end',), tag='record')

        records = []

        try:
            for event, element in context:
                record_data = process_record(element, request_id)
                records.append(record_data)

                if len(records) >= batch_size:
                    insert_records_to_db(records, pg_hook, batch_size)
                    records = []

                element.clear()

            if records:
                insert_records_to_db(records, pg_hook, batch_size)

        except Exception as e:
            update_metadata_status('FAILED', request_id, pg_hook)
            raise AirflowException(f"Ошибка загрузки Данных в таблицу: {str(e)}")


def process_record(record_element, request_id):
    record_data = {
        'request_id': f'{request_id}',
        'id_employee': record_element.findtext('IDсотрудника'),
        'fio': record_element.findtext('ФИО'),
        'id_region': record_element.findtext('IDРегиона'),
        'id_company': record_element.findtext('IDКомпании'),
        'l_faktor': record_element.findtext('Лфактор'),
        'j_faktor': record_element.findtext('Жфактор'),
        'x_faktor': record_element.findtext('Хфактор'),
        'birth_date': record_element.findtext('Датарождения'),
        'date_devations': record_element.findtext('Датаотклонения'),
        'uik': record_element.findtext('УИК'),
        'uik_num': record_element.findtext('УИКЧисло')
    }
    return record_data


def insert_records_to_db(records, pg_hook, batch_size):
    data_to_insert = [(
        record['request_id'], record['id_employee'], record['fio'], record['id_region'],
        record['id_company'], record['l_faktor'], record['j_faktor'], record['x_faktor'],
        record['birth_date'], record['date_devations'], record['uik'], record['uik_num']
    ) for record in records]

    pg_hook.insert_rows(
        table='stg."DWH_DSO_1STGemployees"',
        rows=data_to_insert,
        commit_every=batch_size,
        target_fields=['request_id', 'id_employee', 'fio', 'id_region', 'id_company', 'l_faktor',
                       'j_faktor', 'x_faktor', 'birth_date', 'date_devations', 'uik', 'uik_num']
    )


profiler.disable()
profiler.print_stats(sort='cumulative')

with DAG('load_employees_from_xml_to_stg_stream', default_args=default_args,
         schedule_interval='@once', catchup=False, tags=['test_db']) as dag:

    get_filename_list_task = PythonOperator(
        task_id='get_filename_list',
        python_callable=get_filename_list
    )

    ftp_sensor_task = CustomFTPSensor(
        task_id='ftp_sensor',
        poke_interval=30
    )

    load_metadata_to_staging_task = PythonOperator(
        task_id='load_metadata_to_staging',
        python_callable=load_metadata_to_staging,
    )

    process_large_xml_task = PythonOperator(
        task_id='process_large_xml_and_insert_to_db',
        python_callable=process_large_xml_and_insert_to_db,
        op_kwargs={
            'ftp_conn_id': 'ftp_chtd',
            'postgres_conn_id': 'test_db',
            'batch_size': 16000
        },
    )

    trigger_dag_operator_task = TriggerDagRunOperator(
        task_id='trigger_stream_dag',
        trigger_dag_id='load_employees_from_xml_to_stg_stream',
    )

    get_filename_list_task >> ftp_sensor_task >> load_metadata_to_staging_task >> process_large_xml_task
    process_large_xml_task >> trigger_dag_operator_task
