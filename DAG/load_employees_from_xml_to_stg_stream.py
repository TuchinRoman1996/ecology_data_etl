from airflow import DAG
from airflow.models import BaseOperator
from airflow.operators.python import PythonOperator
from airflow.providers.ftp.hooks.ftp import FTPHook
from airflow.utils.dates import days_ago
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.exceptions import AirflowException
from airflow.utils.decorators import apply_defaults
from lxml import etree
from io import BytesIO

# Задаем переменную с именем таблицы
table = 'DWH_DSO_1STGemployees'

# Оперделяем стандартные аргументы
default_args = {
    'owner': 'bi_master',
    'depends_on_past': False,
    'start_date': days_ago(0),
    'catchup': 'false'
}


# Получаем метаданные для таблицы, указанной в переменной с именем таблицы
def get_metadata(**kwargs):
    pg_hook = PostgresHook('test_db')

    # Записываем метаданные в переменную metadata
    metadata = pg_hook.get_first(f"""
        SELECT request_id, filename
        FROM stg."DWH_DSO_2STGmetadata"
        WHERE table_name = '{table}'
        AND status = 'RUNNING';
    """)

    # Отправляем полученные метаданные в другие задачи
    kwargs['ti'].xcom_push(key='request_id', value=metadata[0])
    kwargs['ti'].xcom_push(key='filename', value=metadata[1])


# Определяем функцию для обновления метаданных заданным статусом по заданному request_id
def update_metadata_status(status, request_id, pg_hook):
    try:
        sql = f"""
            UPDATE stg."DWH_DSO_2STGmetadata"
            SET status = '{status}'
            WHERE request_id = '{request_id}';
        """
        pg_hook.run(sql)
    except Exception as e:
        print(f"Failed to update metadata status: {str(e)}")


# Определяем функцию для потокового чтения данных из файла на FTP и запись этих данных в таблицу
def process_large_xml_and_insert_to_db(ftp_conn_id, postgres_conn_id, batch_size, **kwargs):
    # Получаем метаданные
    request_id = kwargs['ti'].xcom_pull(key='request_id')
    filename = kwargs['ti'].xcom_pull(key='filename')

    # Определяем заданные подключения к FTP и PostgreSQL
    pg_hook = PostgresHook(postgres_conn_id)
    ftp_hook = FTPHook(ftp_conn_id)

    with BytesIO() as xml_buffer:
        # Осуществляем потоковое чтение из файла и записываем полученные данные в буфер с заданным размером
        ftp_hook.retrieve_file(f'for_chtd/test_kxd_glavnivc/Queue/{filename}', xml_buffer,
                               block_size=batch_size * 470)
        xml_buffer.seek(0)  # Передвигамем указатель в начало буфера

        # Определяем парсер, который будет считвывать данные по записям определенных в тэге record
        context = etree.iterparse(xml_buffer, events=('end',), tag='record')

        records = []

        # Находим значение параметров для каждой записи
        try:
            for event, element in context:
                record = (
                    request_id,
                    element.findtext('IDсотрудника'),
                    element.findtext('ФИО'),
                    element.findtext('IDРегиона'),
                    element.findtext('IDКомпании'),
                    element.findtext('Лфактор'),
                    element.findtext('Жфактор'),
                    element.findtext('Хфактор'),
                    element.findtext('Датарождения'),
                    element.findtext('Датаотклонения'),
                    element.findtext('УИК'),
                    element.findtext('УИКЧисло'),
                )
                records.append(record)  # Формируем список

                # Проверяем - достаточно ли записей для вставки
                if len(records) >= batch_size:
                    print('Данные для вставки готовы')
                    insert_records_to_db(records, postgres_conn_id, table)
                    records = []

                element.clear()

            # Вставляем оставшиеся записи
            if records:
                insert_records_to_db(records, postgres_conn_id, table)

            # Меняем статус в таблице метаданных
            update_metadata_status("SUCCESS", request_id, pg_hook)

        except Exception as e:
            # В случае ошибки меняем статус в таблице метаданных
            update_metadata_status('FAILED', request_id, pg_hook)
            raise AirflowException(f"Ошибка загрузки Данных в таблицу: {str(e)}")


# Вставляем данные в таблицу
def insert_records_to_db(records, postgres_conn_id, table_name):
    pg_hook = PostgresHook(postgres_conn_id)

    # Получаем соединение
    conn = pg_hook.get_conn()

    # Создаем курсор
    cursor = conn.cursor()

    try:
        # Получаем список кортежей для пакетной вставки
        values = ', '.join(cursor.mogrify("(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)", record).decode('utf-8')
                           for record in records)

        # Пишем запрос для пакетной вставки
        sql_query = f"""
            INSERT INTO stg."{table_name}"(request_id, id_employee, fio, id_region, id_company, l_faktor, j_faktor, 
            x_faktor, birth_date, date_devations, uik, uik_num)
            VALUES {values};
        """
        cursor.execute(sql_query)  # Выполняем запрос

        conn.commit()  # Применяем изменения
        print('Произошла вставка пакета')

    except Exception as e:
        # Обновляем статус в таблице метаданных в случае ошибки
        update_metadata_status('FAILED', records[0][0], pg_hook)
        raise AirflowException(f"Ошибка загрузки данных в таблицу: {str(e)}")

    finally:
        # Закрываем соединенне
        if conn:
            cursor.close()
            conn.close()


# Создаем собственный оператор который перемещает файл из одной папки на FTP сервере в другую
class FTPMoveFileOperator(BaseOperator):
    # В конструкторе наследуем
    @apply_defaults
    def __init__(self, source_path, destination_path, ftp_conn_id, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.source_path = source_path
        self.destination_path = destination_path
        self.ftp_conn_id = ftp_conn_id

    # Определяем метод который будет выполнять действия на FTP
    def execute(self, context):
        ftp_hook = FTPHook(ftp_conn_id=self.ftp_conn_id)
        filename = context['ti'].xcom_pull(key='filename')

        with ftp_hook.get_conn() as ftp:
            ftp.rename(self.source_path + filename, self.destination_path + filename)
            self.log.info(f"Файл {self.source_path} перемещен в {self.destination_path}")


# Создаем DAG
with DAG('load_employees_from_xml_to_stg_stream', default_args=default_args,
         # access_control={'test_user': {'can_dag_read', 'can_dag_edit'}},
         schedule_interval=None, catchup=False, tags=['test_db']) as dag:
    # Определяем задачу для получения метаданных
    get_metadata_task = PythonOperator(
        task_id='get_metadata',
        python_callable=get_metadata,
        provide_context=True
    )

    # Определяем задачу для загрузки данных из файла в БД
    process_large_xml_task = PythonOperator(
        task_id='process_large_xml_and_insert_to_db',
        python_callable=process_large_xml_and_insert_to_db,
        op_kwargs={
            'ftp_conn_id': 'ftp_chtd',
            'postgres_conn_id': 'test_db',
            'batch_size': 200000
        },
    )

    # Опеределяем задачу для перемещения файла из одной дирректории в другую
    ftp_moved_file_task = FTPMoveFileOperator(
        task_id='ftp_moved_file',
        source_path='for_chtd/test_kxd_glavnivc/Queue/',
        destination_path='for_chtd/test_kxd_glavnivc/Archive/',
        ftp_conn_id='ftp_chtd'
    )

    # Определяем триггер для мониторинга новых файлов
    trigger_ftp_monitoring_dag_task = TriggerDagRunOperator(
        task_id='trigger_ftp_monitoring_dag',
        trigger_dag_id='ftp_monitoring_dag',
    )

    # Определяем триггер для загрузки данных из stg в nds
    trigger_load_employees_from_stg_to_nds = TriggerDagRunOperator(
        task_id='trigger_load_employees_from_stg_to_nds',
        trigger_dag_id='load_employees_from_stg_to_nds',
    )

    get_metadata_task >> process_large_xml_task >> ftp_moved_file_task
    process_large_xml_task >> trigger_ftp_monitoring_dag_task
    process_large_xml_task >> trigger_load_employees_from_stg_to_nds
