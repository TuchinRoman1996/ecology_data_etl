from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.exceptions import AirflowException
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.ftp.hooks.ftp import FTPHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sensors.base import BaseSensorOperator
from airflow.utils.dates import days_ago
import uuid

default_args = {
    'owner': 'RTuchin',
    'depends_on_past': False,
    'start_date': days_ago(0),
    'catchup': 'false'
}


def get_filename_list(**kwargs):
    db_hook = PostgresHook(postgres_conn_id='test_db')

    sql = """
        SELECT filename
        FROM stg."DWH_DSO_2STGmetadata"
        WHERE status not in ('RUNNING', 'FAILED');
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
            ftp_path = "for_chtd/test_kxd_glavnivc/В_очереди"
            ftp_path_encoded = ftp_path.encode('utf-8')
            ftp_files = set(self.ftp_hook.list_directory(ftp_path_encoded.decode('latin-1')))
            print(f'Список файлов в FTP:\n{ftp_files}')
            new_files = ftp_files - self.uploaded_files
            print(f'Новые файлы:{new_files}')

            if new_files:
                context['ti'].xcom_push(key='new_files', value=new_files)
                return True
            else:
                return False

        except AirflowException as e:
            raise AirflowException(f"Ошибка сенсора FTP: {str(e)}")
        finally:
            self.ftp_hook.close_conn()


def select_branch(**kwargs):
    new_files = list(kwargs['ti'].xcom_pull(key='new_files'))
    print(f'Получил новые файлы: {new_files}')

    branches = []

    for file in new_files:
        if 'region' in str(file):
            print(f'Новый файл region: {file}')
            load_metadata_to_staging(file, 'DWH_AO_0regions')
            return 'trigger_load_set_regions_to_stg'
        elif 'companies' in str(file):
            print(f'Новый файл company: {file}')
            load_metadata_to_staging(file, 'DWH_AO_0companies')
            return 'trigger_load_set_companies_to_stg'
        elif 'employee' in str(file):
            print(f'Новый файл employee: {file}')
            load_metadata_to_staging(file, 'DWH_DSO_1STGemployees')
            return 'trigger_load_employee_to_stg'

    return branches


def load_metadata_to_staging(filename, table_name):
    request_id = str(uuid.uuid4())
    print(f'Получил название файла: {filename}')
    pg_hook = PostgresHook(postgres_conn_id='test_db')

    sql = f"""
        INSERT INTO stg."DWH_DSO_2STGmetadata"(request_id, table_name, filename)
        VALUES('{request_id}', '{table_name}', '{filename}');
    """
    pg_hook.run(sql)


with DAG('ftp_monitoring_dag', default_args=default_args, schedule_interval='@once', catchup=False,
         tags=['test_db']) as dag:
    get_filename_list_task = PythonOperator(
        task_id='get_filename_list',
        python_callable=get_filename_list,
    )

    ftp_sensor_task = CustomFTPSensor(
        task_id='ftp_sensor',
        poke_interval=30
    )

    branch_task = BranchPythonOperator(
        task_id='branch_task',
        provide_context=True,
        python_callable=select_branch
    )

    trigger_load_employee_to_stg = TriggerDagRunOperator(
        task_id='trigger_load_employee_to_stg',
        trigger_dag_id='load_employees_from_xml_to_stg_stream',
    )

    trigger_load_set_regions_to_stg = TriggerDagRunOperator(
        task_id='trigger_load_set_regions_to_stg',
        trigger_dag_id='load_set_regions_from_xml_to_stg',
    )

    trigger_load_set_companies_to_stg = TriggerDagRunOperator(
        task_id='trigger_load_set_companies_to_stg',
        trigger_dag_id='load_set_companies_from_xml_to_stg',
    )

    get_filename_list_task >> ftp_sensor_task >> branch_task >> [trigger_load_employee_to_stg,
                                                                 trigger_load_set_regions_to_stg,
                                                                 trigger_load_set_companies_to_stg]
