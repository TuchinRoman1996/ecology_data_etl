from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.exceptions import AirflowException
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.ftp.hooks.ftp import FTPHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sensors.base import BaseSensorOperator
from airflow.utils.dates import days_ago
import uuid

# Определяем стандартные параметры дага
default_args = {
    'owner': 'bi_master',
    'depends_on_past': False,
    'start_date': days_ago(0),
    'catchup': 'false'
}


# Функция которая получает список файлов, уже загруженных в базу
def get_filename_list(**kwargs):
    # Подключаюсь к бд с помощью преднастроенного подключения
    db_hook = PostgresHook(postgres_conn_id='test_db')
    # Формируем запрос в базу который получает список файлов из бд
    sql = """
        SELECT filename
        FROM stg."DWH_DSO_2STGmetadata"
        WHERE status not in ('RUNNING', 'FAILED');
    """
    results = db_hook.get_records(sql)  # Возвращаем результат запроса в список
    if results:
        # Формируем множество на основе списка
        filenames = set(result[0] for result in results)
    else:
        filenames = set()

    print(f'Список файлов в БД:\n{filenames}')
    # Отправляем список имен в XCom
    kwargs['ti'].xcom_push(key='filename_list', value=filenames)


# Определяем кастомный сенсор, который будет мониторить наш FTP на наличие новых файлов
class CustomFTPSensor(BaseSensorOperator):
    def __init__(self, *args, **kwargs):  # Определяем конструктор
        super().__init__(*args, **kwargs)  # Наследуем методы родительского класса
        self.ftp_hook = None
        self.uploaded_files = None
        self.ftp_files = None

    # Определяем стандартный метод poke, который будет выполнятся с заданным интервалом времени
    def poke(self, context):
        # Используем преднастроенное подключение
        self.ftp_hook = FTPHook(ftp_conn_id='ftp_chtd')
        # Получаем список загруженных файлов (можно оптимизировать)
        self.uploaded_files = context['ti'].xcom_pull(key='filename_list')

        try:
            # Получение списка файлов с FTP
            ftp_files = set(self.ftp_hook.list_directory("for_chtd/test_kxd_glavnivc/Queue"))
            print(f'Список файлов в FTP:\n{ftp_files}')
            # Вычитаем список загруженных файлов из файлов на FTP
            new_files = ftp_files - self.uploaded_files
            print(f'Новые файлы:{new_files}')

            if new_files:
                # Передаем список новых файдов в XCom
                context['ti'].xcom_push(key='new_files', value=new_files)
                return True
            else:
                return False

        except AirflowException as e:
            # Выдаем сообщение в случае ощибки
            raise AirflowException(f"Ошибка сенсора FTP: {str(e)}")
        finally:
            # Закрываем соединение (можно оптимизировать)
            self.ftp_hook.close_conn()


# Функция которая выбирает даг который нужно триггерить
def select_branch(**kwargs):
    # Получаем список новых файлов
    new_files = list(kwargs['ti'].xcom_pull(key='new_files'))
    print(f'Получил новые файлы: {new_files}')

    branches = []

    # Определяем какой даг будет запускаться на основе названия файлов (можно задать приоритет)
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


# Функия, которая генерит метаданные для вставки в stg
def load_metadata_to_staging(filename, table_name):
    request_id = str(uuid.uuid4())  # Генерим uuid
    print(f'Получил название файла: {filename}')
    pg_hook = PostgresHook(postgres_conn_id='test_db')

    # Запрос который вставляет данные в таблицу метаданных
    sql = f"""
        INSERT INTO stg."DWH_DSO_2STGmetadata"(request_id, table_name, filename)
        VALUES('{request_id}', '{table_name}', '{filename}');
    """
    pg_hook.run(sql)


# Определяем настройки DAG
with DAG('ftp_monitoring_dag', default_args=default_args, schedule_interval=None, catchup=False,
         tags=['test_db']) as dag:

    # Определяем задачу на получение списка файлов
    get_filename_list_task = PythonOperator(
        task_id='get_filename_list',
        python_callable=get_filename_list,
    )

    # Опредеделяем задачу с нашим кастомным сенсором
    ftp_sensor_task = CustomFTPSensor(
        task_id='ftp_sensor',
        poke_interval=30
    )

    # Определяем задачу которая триггерит соответствующий DAG в зависимости от названия файла
    branch_task = BranchPythonOperator(
        task_id='branch_task',
        provide_context=True,
        python_callable=select_branch
    )

    # Определяем триггер для загрузки сотрудников
    trigger_load_employee_to_stg = TriggerDagRunOperator(
        task_id='trigger_load_employee_to_stg',
        trigger_dag_id='load_employees_from_xml_to_stg_stream',
    )

    # Определяем триггер для загрузки регионов
    trigger_load_set_regions_to_stg = TriggerDagRunOperator(
        task_id='trigger_load_set_regions_to_stg',
        trigger_dag_id='load_set_regions_from_xml_to_stg',
    )

    # Определяем триггер для загрузки компаний
    trigger_load_set_companies_to_stg = TriggerDagRunOperator(
        task_id='trigger_load_set_companies_to_stg',
        trigger_dag_id='load_set_companies_from_xml_to_stg',
    )

    get_filename_list_task >> ftp_sensor_task >> branch_task >> [trigger_load_employee_to_stg,
                                                                 trigger_load_set_regions_to_stg,
                                                                 trigger_load_set_companies_to_stg]
