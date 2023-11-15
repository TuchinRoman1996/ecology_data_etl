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

table = 'DWH_DSO_1STGemployees'

default_args = {
    'owner': 'RTuchin',
    'depends_on_past': False,
    'start_date': days_ago(0),
    'catchup': 'false'
}


def get_metadata(**kwargs):
    pg_hook = PostgresHook('test_db')
    metadata = pg_hook.get_first("""
        SELECT request_id, filename
        FROM stg."DWH_DSO_2STGmetadata"
        WHERE table_name = 'DWH_DSO_1STGemployees'
        AND status = 'RUNNING';
    """)

    kwargs['ti'].xcom_push(key='request_id', value=metadata[0])
    kwargs['ti'].xcom_push(key='filename', value=metadata[1])


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


def process_large_xml_and_insert_to_db(ftp_conn_id, postgres_conn_id, batch_size, **kwargs):
    request_id = kwargs['ti'].xcom_pull(key='request_id')
    filename = kwargs['ti'].xcom_pull(key='filename')

    pg_hook = PostgresHook(postgres_conn_id)
    ftp_hook = FTPHook(ftp_conn_id)

    def block_info():
        print('Блок получен')

    with BytesIO() as xml_buffer:
        ftp_hook.retrieve_file(f'for_chtd/test_kxd_glavnivc/В_очереди/{filename}', xml_buffer, callback=block_info(),
                               block_size=batch_size * 470)
        xml_buffer.seek(0)

        context = etree.iterparse(xml_buffer, events=('end',), tag='record')

        records = []

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
                records.append(record)

                if len(records) >= batch_size:
                    print('Данные для вставки готовы')
                    insert_records_to_db(records, postgres_conn_id, table)
                    records = []

                element.clear()

            if records:
                insert_records_to_db(records, postgres_conn_id, table)

            update_metadata_status("SUCCESS", request_id, pg_hook)

        except Exception as e:
            update_metadata_status('FAILED', request_id, pg_hook)
            raise AirflowException(f"Ошибка загрузки Данных в таблицу: {str(e)}")


def insert_records_to_db(records, postgres_conn_id, table_name):
    pg_hook = PostgresHook(postgres_conn_id)

    # Получаем соединение
    conn = pg_hook.get_conn()

    # Создаем курсор
    cursor = conn.cursor()

    try:

        values = ', '.join(cursor.mogrify("(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)", record).decode('utf-8')
                           for record in records)

        sql_query = f"""
            INSERT INTO stg."{table_name}"(request_id, id_employee, fio, id_region, id_company, l_faktor, j_faktor, 
            x_faktor, birth_date, date_devations, uik, uik_num)
            VALUES {values};
        """
        cursor.execute(sql_query)

        conn.commit()
        print('Произошла вставка пакета')

    except Exception as e:
        update_metadata_status('FAILED', records[0][0], pg_hook)
        raise AirflowException(f"Ошибка загрузки данных в таблицу: {str(e)}")

    finally:
        if conn:
            cursor.close()
            conn.close()


class FTPMoveFileOperator(BaseOperator):
    @apply_defaults
    def __init__(self, source_path, destination_path, ftp_conn_id, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.source_path = source_path
        self.destination_path = destination_path
        self.ftp_conn_id = ftp_conn_id

    def execute(self, context):
        ftp_hook = FTPHook(ftp_conn_id=self.ftp_conn_id)
        filename = context['ti'].xcom_pull(key='filename')

        with ftp_hook.get_conn() as ftp:
            ftp.rename(self.source_path + filename, self.destination_path + filename)
            self.log.info(f"Файл {self.source_path} перемещен в {self.destination_path}")


with DAG('load_employees_from_xml_to_stg_stream', default_args=default_args,
         # access_control={'test_user': {'can_dag_read', 'can_dag_edit'}},
         schedule_interval=None, catchup=False, tags=['test_db']) as dag:
    get_metadata_task = PythonOperator(
        task_id='get_metadata',
        python_callable=get_metadata,
        provide_context=True
    )

    process_large_xml_task = PythonOperator(
        task_id='process_large_xml_and_insert_to_db',
        python_callable=process_large_xml_and_insert_to_db,
        op_kwargs={
            'ftp_conn_id': 'ftp_chtd',
            'postgres_conn_id': 'test_db',
            'batch_size': 200000
        },
    )

    ftp_moved_file_task = FTPMoveFileOperator(
        task_id='ftp_moved_file',
        source_path='for_chtd/test_kxd_glavnivc/В_очереди/',
        destination_path='for_chtd/test_kxd_glavnivc/Архив/',
        ftp_conn_id='ftp_chtd'
    )

    trigger_ftp_monitoring_dag_task = TriggerDagRunOperator(
        task_id='trigger_ftp_monitoring_dag',
        trigger_dag_id='ftp_monitoring_dag',
    )

    trigger_load_employees_from_stg_to_nds = TriggerDagRunOperator(
        task_id='trigger_load_employees_from_stg_to_nds',
        trigger_dag_id='load_employees_from_stg_to_nds',
    )

    get_metadata_task >> process_large_xml_task >> ftp_moved_file_task
    process_large_xml_task >> trigger_ftp_monitoring_dag_task
    process_large_xml_task >> trigger_load_employees_from_stg_to_nds
