from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.ftp.hooks.ftp import FTPHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.dates import days_ago
from airflow.utils.decorators import apply_defaults
from lxml import etree
from io import BytesIO

# Определяем таблицу загрузки
table_name = 'DWH_AO_0regions'


# Опеределяем функцию для обновления метаданных
def update_metadata_status(status, request_id, pg_hook):
    print(f'status: {status}\nrequest_id: {request_id}\npg_hook: {pg_hook}')
    try:
        sql = f"""
            UPDATE stg."DWH_DSO_2STGmetadata"
            SET status = '{status}'
            WHERE request_id = '{request_id}'; 
        """
        pg_hook.run(sql)
    except Exception as e:
        print(f"Failed to update metadata status: {str(e)}")


# Определяем функцию для получения метаданных
def get_metadata(table, **kwargs):
    pg_hook = PostgresHook('test_db')
    metadata = pg_hook.get_first(f"""
        SELECT request_id, filename
        FROM stg."DWH_DSO_2STGmetadata"
        WHERE table_name = '{table}'
        AND status = 'RUNNING';
    """)

    kwargs['ti'].xcom_push(key='request_id', value=metadata[0])
    kwargs['ti'].xcom_push(key='filename', value=metadata[1])


# Определяем функцию для загрузки данных в справочник regions
def load_set_regions_to_stg(ftp_conn_id, postgres_conn_id, batch_size, **kwargs):
    # Получаем метаданные
    request_id = kwargs['ti'].xcom_pull(key='request_id')
    filename = kwargs['ti'].xcom_pull(key='filename')

    # Исползуем преднастроенные подулючения
    pg_hook = PostgresHook(postgres_conn_id)
    ftp_hook = FTPHook(ftp_conn_id)

    with BytesIO() as xml_buffer:
        # Читаем файл и записываем в буфер
        ftp_hook.retrieve_file(f'for_chtd/test_kxd_glavnivc/В_очереди/{filename}', xml_buffer)
        xml_buffer.seek(0)  # Передвигаем указатель в начало буфера

        # Определяем парсер
        context = etree.iterparse(xml_buffer, events=('end',), tag='record')

        records = []

        # Осуществляем пакетную вставку
        try:
            for event, element in context:
                record_data = process_record(element, request_id)
                records.append(record_data)

                if len(records) >= batch_size:
                    insert_records_to_db(records, pg_hook, batch_size, table_name)
                    records = []

                element.clear()

            if records:
                insert_records_to_db(records, pg_hook, batch_size, table_name)

        except Exception as e:
            update_metadata_status('FAILED', request_id, pg_hook)
            raise AirflowException(f"Ошибка загрузки Данных в таблицу: {str(e)}")


# Парсим наш XML
def process_record(record_element, request_id):
    record_data = {
        'request_id': f'{request_id}',
        'region_name': record_element.findtext('НазваниеРегиона'),
        'id_region': record_element.findtext('IDРегиона'),
        'region_parent': record_element.findtext('ФО'),
        'population': record_element.findtext('ГражданКол-во'),
        'name_eng': record_element.findtext('Name'),
        'code': record_element.findtext('Code'),
        'region_oktmo': record_element.findtext('ОКТМО'),
        'region_okato': record_element.findtext('ОКАТО'),
        'f_value': record_element.findtext('Ф_Значение'),
    }
    return record_data


# Вставляем данные в таблицу
def insert_records_to_db(records, pg_hook, batch_size, table):
    data_to_insert = [(
        record['request_id'], record['region_name'], record['id_region'], record['region_parent'],
        record['population'], record['name_eng'], record['code'], record['region_oktmo'],
        record['region_okato'], record['f_value']
    ) for record in records]

    pg_hook.insert_rows(
        table=f'stg."{table}"',
        rows=data_to_insert,
        commit_every=batch_size,
        target_fields=['request_id', 'region_name', 'id_region', 'region_parent', 'population', 'name_eng',
                       'code', 'region_oktmo', 'region_okato', 'f_value']
    )


# Создаем оператор для перемещения файла из одной папки в другую
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


# Определяем стандартные аргументы
default_args = {
    'owner': 'RTuchin',
    'depends_on_past': False,
    'start_date': days_ago(0),
    'catchup': 'false'
}

# Определяем DAG
with DAG('load_set_regions_from_xml_to_stg', default_args=default_args, schedule_interval=None,
         catchup=False, tags=['test_db']) as dag:

    # Получаем метаданные
    get_metadata_task = PythonOperator(
        task_id='get_metadata',
        provide_context=True,
        python_callable=get_metadata,
        op_kwargs={
            'table': table_name
        }
    )

    # Загружаем данные в таблицу БД
    load_regions_to_stg = PythonOperator(
        task_id='load_regions_to_stg',
        python_callable=load_set_regions_to_stg,
        op_kwargs={
            'ftp_conn_id': 'ftp_chtd',
            'postgres_conn_id': 'test_db',
            'batch_size': 1000
        },
    )

    # Обновляем статус метаданных
    update_metadata_status_success_task = PythonOperator(
        task_id='update_metadata_status_success',
        python_callable=update_metadata_status,
        op_args=['SUCCESS', '{{ ti.xcom_pull(key="request_id") }}', PostgresHook('test_db')]
    )

    # Перемещаем файл
    ftp_moved_file_task = FTPMoveFileOperator(
        task_id='ftp_moved_file',
        source_path='for_chtd/test_kxd_glavnivc/В_очереди/',
        destination_path='for_chtd/test_kxd_glavnivc/Архив/',
        ftp_conn_id='ftp_chtd'
    )

    # Загружаем данные на слой nds
    load_set_regions_from_stg_to_nds_task = PostgresOperator(
        task_id='load_set_regions_from_stg_to_nds',
        postgres_conn_id='test_db',
        sql="""
            INSERT INTO nds."DWH_AO_HNDSregions"
            (id_region, region_name, region_parent, population, name_eng, code, region_oktmo, region_okato, f_value)
            with last_record as (
                select request_id 
                from stg."DWH_DSO_2STGmetadata" dds 
                where table_name = 'DWH_AO_0regions'
                order by load_date desc 
                limit 1
            )
            select 
                id_region::bigint
                ,region_name::text 
                ,region_parent::text
                ,population::numeric(14)
                ,name_eng::text
                ,code::int8
                ,region_oktmo::int8
                ,region_okato ::int8
                ,case when f_value ='' then null
                else f_value::numeric(13)
                end
            from stg."DWH_AO_0regions" r
            join last_record lr on r.request_id = lr.request_id
            on conflict (id_region) do update SET
              region_name = EXCLUDED.region_name,
              region_parent = EXCLUDED.region_parent,
              population = EXCLUDED.population,
              name_eng = EXCLUDED.name_eng,
              code = EXCLUDED.code,
              region_oktmo = EXCLUDED.region_oktmo,
              region_okato = EXCLUDED.region_okato,
              f_value = EXCLUDED.f_value;
        """
    )

    # Триггерим даг моноиторинга
    trigger_ftp_monitoring_dag_task = TriggerDagRunOperator(
        task_id='trigger_ftp_monitoring_dag',
        trigger_dag_id='ftp_monitoring_dag',
    )

    get_metadata_task >> load_regions_to_stg >> update_metadata_status_success_task >> trigger_ftp_monitoring_dag_task
    load_regions_to_stg >> load_set_regions_from_stg_to_nds_task >> trigger_ftp_monitoring_dag_task
    load_regions_to_stg >> ftp_moved_file_task >> trigger_ftp_monitoring_dag_task
