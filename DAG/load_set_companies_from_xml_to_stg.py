from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.ftp.hooks.ftp import FTPHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.dates import days_ago
from lxml import etree
from io import BytesIO

table_name = 'DWH_AO_0companies'


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


def load_set_companies_to_stg(ftp_conn_id, postgres_conn_id, batch_size, **kwargs):
    request_id = kwargs['ti'].xcom_pull(key='request_id')
    filename = kwargs['ti'].xcom_pull(key='filename')

    pg_hook = PostgresHook(postgres_conn_id)
    ftp_hook = FTPHook(ftp_conn_id)

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
                    insert_records_to_db(records, pg_hook, batch_size, table_name)
                    records = []

                element.clear()

            if records:
                insert_records_to_db(records, pg_hook, batch_size, table_name)

        except Exception as e:
            update_metadata_status('FAILED', request_id, pg_hook)
            raise AirflowException(f"Ошибка загрузки Данных в таблицу: {str(e)}")


def process_record(record_element, request_id):
    record_data = {
        'request_id': f'{request_id}',
        'id_company': record_element.findtext('IDКомпании'),
        'company_parent': record_element.findtext('ГоловнаяОрганизация'),
        'id_region': record_element.findtext('idРегиона'),
        'type_company': record_element.findtext('ТипКомпании'),
        'type_industry': record_element.findtext('ТипОтрасли'),
        'rosstat': record_element.findtext('Росстат'),
        'create_date': record_element.findtext('Датаcозданияорг'),
        'date2': record_element.findtext('Дата2'),
        'inn': record_element.findtext('ИНН'),
    }
    return record_data


def insert_records_to_db(records, pg_hook, batch_size, table):
    data_to_insert = [(
        record['request_id'], record['id_company'], record['company_parent'], record['id_region'],
        record['type_company'], record['type_industry'], record['rosstat'], record['create_date'],
        record['date2'], record['inn']
    ) for record in records]

    pg_hook.insert_rows(
        table=f'stg."{table}"',
        rows=data_to_insert,
        commit_every=batch_size,
        target_fields=['request_id', 'id_company', 'company_parent', 'id_region', 'type_company', 'type_industry',
                       'rosstat', 'create_date', 'date2', 'inn']
    )


default_args = {
    'owner': 'Rtuchin',
    'depends_on_past': False,
    'start_date': days_ago(0),
    'catchup': 'false'
}

with DAG('load_set_companies_from_xml_to_stg', default_args=default_args, schedule_interval='@once',
         catchup=False, tags=['test_db']) as dag:

    get_metadata_task = PythonOperator(
        task_id='get_metadata',
        provide_context=True,
        python_callable=get_metadata,
        op_kwargs={
            'table': table_name
        }
    )

    load_companies_to_stg = PythonOperator(
        task_id='load_companies_to_stg',
        python_callable=load_set_companies_to_stg,
        op_kwargs={
            'ftp_conn_id': 'ftp_chtd',
            'postgres_conn_id': 'test_db',
            'batch_size': 1000
        },
    )

    update_metadata_status_success_task = PythonOperator(
        task_id='update_metadata_status_success',
        python_callable=update_metadata_status,
        op_args=['SUCCESS', '{{ ti.xcom_pull(key="request_id") }}', PostgresHook('test_db')]
    )

    trigger_ftp_monitoring_dag_task = TriggerDagRunOperator(
        task_id='trigger_ftp_monitoring_dag',
        trigger_dag_id='ftp_monitoring_dag',
    )

    get_metadata_task >> load_companies_to_stg >> update_metadata_status_success_task >> trigger_ftp_monitoring_dag_task
