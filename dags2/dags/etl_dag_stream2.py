import sys
from os.path import abspath, dirname

from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.decorators import apply_defaults

sys.path.append(dirname(dirname(abspath(__file__))))

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator
from airflow.models import Connection
from airflow.sensors.base import BaseSensorOperator
from psycopg2.extras import RealDictCursor
from psycopg2 import OperationalError

from scripts.extract_from_postgres import extract_from_postgresql_club_ru
from scripts.load_to_postgres import load_to_stg_age_group, load_to_stg_city, \
    load_to_stg_country, load_to_stg_customer, load_to_stg_invoice_line, load_to_stg_region, \
    load_to_stg_reservation_line, load_to_stg_reservations, load_to_stg_resort, load_to_stg_sales, \
    load_to_stg_sales_person, load_to_stg_sales_service, load_to_stg_sales_service_line, load_to_stg_region_sline


class PostgresNotificationSensor(BaseSensorOperator):
    @apply_defaults
    def __init__(self, *args, **kwargs):
        super(PostgresNotificationSensor, self).__init__(*args, **kwargs)
        self.cursor_source = None
        self.cursor_target = None
        self.conn_source = None
        self.conn_target = None
        self.previous_count = 0

    def _get_connection(self, conn_id):
        hook = PostgresHook(postgres_conn_id=conn_id)
        conn = hook.get_conn()
        return conn

    def _close_connection(self, conn, cursor):
        cursor.close()
        conn.close()

    def poke(self, context):
        if not self.conn_source:
            self.conn_source = self._get_connection('club_ru')
            self.cursor_source = self.conn_source.cursor(cursor_factory=RealDictCursor)

        if not self.conn_target:
            self.conn_target = self._get_connection('target_db')
            self.cursor_target = self.conn_target.cursor(cursor_factory=RealDictCursor)

        while True:
            try:
                self.cursor_target.execute("SELECT COUNT(*) FROM stg.invoice_line_club")
                self.previous_count = self.cursor_target.fetchone()['count']
                self.cursor_source.execute("SELECT COUNT(*) FROM invoice_line")
                current_count = self.cursor_source.fetchone()['count']

                print(f'Кол-во строк на источнике {current_count}')
                print(f'Кол-во строк в целевой: {self.previous_count}')

                if current_count != self.previous_count:
                    self.log.info("Количество записей изменилось. Выполняем дополнительные действия.")
                    self.previous_count = current_count

                    self.cursor_target.execute("SELECT max(concat(inv_id, service_id)) FROM stg.invoice_line_club;")
                    self.cursor_source.execute(f"""
                                            SELECT concat(inv_id, service_id) compose_key
                                            FROM public.invoice_line
                                            WHERE concat(inv_id, service_id)::bigint > {self.cursor_target.fetchone()['max']};""")

                    self.xcom_push(context, key='compose_key', value=self.cursor_source.fetchall()['compose_key'])
                    print(self.cursor_source.fetchall()['compose_key'])

            except OperationalError as e:
                self.log.error(f"Ошибка прослушки: {str(e)}")
                return False
            finally:
                self._close_connection(self.conn_source, self.cursor_source)
                self._close_connection(self.conn_target, self.cursor_target)


source_db_conn = Connection.get_connection_from_secrets('club_ru')
source_db_params = {
    'database': source_db_conn.extra_dejson.get('database'),
    'user': source_db_conn.login,
    'password': source_db_conn.password,
    'host': source_db_conn.host,
    'port': source_db_conn.port,
}

target_db_conn = Connection.get_connection_from_secrets('target_db')
target_db_params = {
    'database': 'ecology_analytics',
    'user': target_db_conn.login,
    'password': target_db_conn.password,
    'host': target_db_conn.host,
    'port': target_db_conn.port,
}

default_args = {
    'owner': 'Username',
    'depends_on_past': False,
    'start_date': days_ago(0),
    'catchup': 'false'
}

with DAG('Stream_2', default_args=default_args, schedule_interval='@once', schedule=None, concurrency=16) as dag:
    listen_source_db = PostgresNotificationSensor(
        task_id='listen_source_db'
    )

    extract_from_postgres_ps2 = PythonOperator(
        task_id='extract_from_postgres_ps2',
        python_callable=extract_from_postgresql_club_ru,
        provide_context=True,
        op_args=[source_db_params]
    )

    load_to_stg_age_group = PythonOperator(
        task_id='load_to_stg_age_group',
        python_callable=load_to_stg_age_group,
        op_args=[target_db_params]
    )

    load_to_stg_city = PythonOperator(
        task_id='load_to_stg_city',
        python_callable=load_to_stg_city,
        op_args=[target_db_params]
    )

    load_to_stg_country = PythonOperator(
        task_id='load_to_stg_country',
        python_callable=load_to_stg_country,
        op_args=[target_db_params]
    )

    load_to_stg_customer = PythonOperator(
        task_id='load_to_stg_customer',
        python_callable=load_to_stg_customer,
        op_args=[target_db_params]
    )

    load_to_stg_invoice_line = PythonOperator(
        task_id='load_to_stg_invoice_line',
        python_callable=load_to_stg_invoice_line,
        op_args=[target_db_params]
    )

    load_to_stg_region = PythonOperator(
        task_id='load_to_stg_region',
        python_callable=load_to_stg_region,
        op_args=[target_db_params]
    )

    load_to_stg_region_sline = PythonOperator(
        task_id='load_to_stg_region_sline',
        python_callable=load_to_stg_region_sline,
        op_args=[target_db_params]
    )

    load_to_stg_reservation_line = PythonOperator(
        task_id='load_to_stg_reservation_line',
        python_callable=load_to_stg_reservation_line,
        op_args=[target_db_params]
    )

    load_to_stg_reservations = PythonOperator(
        task_id='load_to_stg_reservations',
        python_callable=load_to_stg_reservations,
        op_args=[target_db_params]
    )

    load_to_stg_resort = PythonOperator(
        task_id='load_to_stg_resort',
        python_callable=load_to_stg_resort,
        op_args=[target_db_params]
    )

    load_to_stg_sales = PythonOperator(
        task_id='load_to_stg_sales',
        python_callable=load_to_stg_sales,
        op_args=[target_db_params]
    )

    load_to_stg_sales_person = PythonOperator(
        task_id='load_to_stg_sales_person',
        python_callable=load_to_stg_sales_person,
        op_args=[target_db_params]
    )

    load_to_stg_sales_service = PythonOperator(
        task_id='load_to_stg_sales_service',
        python_callable=load_to_stg_sales_service,
        op_args=[target_db_params]
    )

    load_to_stg_sales_service_line = PythonOperator(
        task_id='load_to_stg_sales_service_line',
        python_callable=load_to_stg_sales_service_line,
        op_args=[target_db_params]
    )

    load_to_nds = PostgresOperator(
        task_id='load_to_nds',
        postgres_conn_id='target_db',
        database='ecology_analytics',
        sql="""
            select  nds.insert_dim_resort();
            select  nds.insert_dim_seller();
            select  nds.insert_dim_service();
            select  nds.insert_dim_country();
            select  nds.insert_dim_service_line();
            select  nds.insert_dim_customer();
            select  nds.insert_fact_sale_data();
        """
    )

    load_to_ods = PostgresOperator(
        task_id='load_to_ods',
        postgres_conn_id='target_db',
        database='ecology_analytics',
        sql="""
            select  ods.insert_dim_resort();
            select  ods.insert_dim_seller();
            select  ods.insert_dim_service();
            select  ods.insert_dim_country();
            select  ods.insert_dim_service_line();
            select  ods.insert_dim_customer();
            select  ods.insert_fact_sale_data();
        """
    )

    load_to_dds_sales_with_moving_average = PostgresOperator(
        task_id='load_to_dds_sales_with_moving_average',
        postgres_conn_id='target_db',
        database='ecology_analytics',
        sql="""
            truncate dds.sales_with_moving_average;
            WITH sales_data AS (
                SELECT
                    EXTRACT(YEAR FROM fs.invoice_date) AS sale_year,
                    EXTRACT(MONTH FROM fs.invoice_date) AS sale_month,
                    SUM(fs.price) AS total_sales
                FROM ods.fact_sale fs
                GROUP BY sale_year, sale_month
            )
            INSERT INTO dds.sales_with_moving_average (sale_year, sale_month, total_sales, moving_average)
            SELECT sale_year, sale_month, total_sales, AVG(total_sales) OVER (ORDER BY sale_year, sale_month ROWS BETWEEN 3 PRECEDING AND 1 FOLLOWING) AS moving_average
            FROM sales_data;
        """
    )

    load_to_dds_sales_growth_by_month = PostgresOperator(
        task_id='load_to_dds_sales_growth_by_month',
        postgres_conn_id='target_db',
        database='ecology_analytics',
        sql="""
            truncate dds.sales_growth_by_month;  
            WITH monthly_sales AS (
                SELECT
                    EXTRACT(YEAR FROM fs.invoice_date) AS sale_year,
                    EXTRACT(MONTH FROM fs.invoice_date) AS sale_month,
                    SUM(fs.price) AS total_sales
                FROM ods.fact_sale fs
                GROUP BY sale_year, sale_month
            )
            INSERT INTO dds.sales_growth_by_month (sale_year, sale_month, current_month_sales, previous_month_sales, sales_growth)
            SELECT ms1.sale_year, ms1.sale_month, ms1.total_sales AS current_month_sales, ms2.total_sales AS previous_month_sales,
                (ms1.total_sales - ms2.total_sales) AS sales_growth
            FROM monthly_sales ms1
            LEFT JOIN monthly_sales ms2 ON ms1.sale_year = ms2.sale_year AND ms1.sale_month = ms2.sale_month + 1;
        """
    )

    load_to_dds_average_sales_by_day_of_week = PostgresOperator(
        task_id='load_to_dds_average_sales_by_day_of_week',
        postgres_conn_id='target_db',
        database='ecology_analytics',
        sql="""
            truncate dds.average_sales_by_day_of_week;
            INSERT INTO dds.average_sales_by_day_of_week (day_of_week, average_sale)
            SELECT 
              CASE 
                WHEN EXTRACT(DOW FROM fs.invoice_date) = 0 THEN 'Понедельник'
                WHEN EXTRACT(DOW FROM fs.invoice_date) = 1 THEN 'Вторник'
                WHEN EXTRACT(DOW FROM fs.invoice_date) = 2 THEN 'Среда'
                WHEN EXTRACT(DOW FROM fs.invoice_date) = 3 THEN 'Четверг'
                WHEN EXTRACT(DOW FROM fs.invoice_date) = 4 THEN 'Пятница'
                WHEN EXTRACT(DOW FROM fs.invoice_date) = 5 THEN 'Суббота'
                WHEN EXTRACT(DOW FROM fs.invoice_date) = 6 THEN 'Воскресенье'
                ELSE 'Неизвестный день' 
              END AS day_of_week,
              AVG(fs.price) AS average_sale
            FROM ods.fact_sale fs
            GROUP BY day_of_week;
        """
    )

    load_to_dds_sales_trends_by_month = PostgresOperator(
        task_id='load_to_dds_sales_trends_by_month',
        postgres_conn_id='target_db',
        database='ecology_analytics',
        sql="""
            truncate dds.sales_trends_by_month;
            INSERT INTO dds.sales_trends_by_month (sale_year, sale_month, total_sales)
            SELECT EXTRACT(YEAR FROM fs.invoice_date) AS sale_year,
                EXTRACT(MONTH FROM fs.invoice_date) AS sale_month,
                SUM(fs.price) AS total_sales
            FROM ods.fact_sale fs
            GROUP BY sale_year, sale_month;
        """
    )

    load_to_dds_average_sales_by_seller = PostgresOperator(
        task_id='load_to_dds_average_sales_by_seller',
        postgres_conn_id='target_db',
        database='ecology_analytics',
        sql="""
            truncate dds.average_sales_by_seller;
            INSERT INTO dds.average_sales_by_seller (sales_person, average_sale)
            SELECT ds.sales_person, AVG(fs.price) AS average_sale
            FROM ods.fact_sale fs
            JOIN ods.dim_seller ds ON fs.seller_id = ds.seller_id
            GROUP BY ds.sales_person;
        """
    )

    load_to_dds_sales_by_service = PostgresOperator(
        task_id='load_to_dds_sales_by_service',
        postgres_conn_id='target_db',
        database='ecology_analytics',
        sql="""
            truncate dds.sales_by_service;
            INSERT INTO dds.sales_by_service (service, total_sales)
            SELECT ds.service, SUM(fs.price) AS total_sales
            FROM ods.fact_sale fs
            JOIN ods.dim_service ds ON fs.service_id = ds.service_id
            GROUP BY ds.service;
        """
    )

    load_to_dds_customer_age_groups = PostgresOperator(
        task_id='load_to_dds_customer_age_groups',
        postgres_conn_id='target_db',
        database='ecology_analytics',
        sql="""
            truncate dds.customer_age_groups;
            INSERT INTO dds.customer_age_groups (age_group, customer_count)
            SELECT
            CASE
                WHEN dc.age < 10 THEN 'Under 10'
                WHEN dc.age BETWEEN 10 AND 19 THEN '10-19'
                WHEN dc.age BETWEEN 20 AND 29 THEN '20-29'
                WHEN dc.age BETWEEN 30 AND 39 THEN '30-39'
                WHEN dc.age BETWEEN 40 AND 49 THEN '40-49'
                WHEN dc.age BETWEEN 50 AND 59 THEN '50-59'
                WHEN dc.age BETWEEN 60 AND 69 THEN '60-69'
                WHEN dc.age BETWEEN 70 AND 79 THEN '70-79'
                WHEN dc.age >= 80 THEN '80 and Over'
                ELSE 'Unknown'
            END AS age_group,
            COUNT(*) AS customer_count
        FROM ods.dim_customer dc
        GROUP BY age_group;
        """
    )

    load_to_dds_sales_by_country_and_region = PostgresOperator(
        task_id='load_to_dds_sales_by_country_and_region',
        postgres_conn_id='target_db',
        database='ecology_analytics',
        sql="""
            truncate dds.sales_by_country_and_region;
            INSERT INTO dds.sales_by_country_and_region (country, resort, invoice_date, total_sales)
            SELECT dc.country, dr.resort, fs.invoice_date, SUM(fs.price) AS total_sales
            FROM ods.fact_sale fs
            JOIN ods.dim_country dc ON fs.country_id = dc.country_id
            JOIN ods.dim_resort dr ON fs.resort_id = dr.resort_id
            GROUP BY dc.country, dr.resort, fs.invoice_date;
        """
    )

    load_to_dds_sales_by_employee = PostgresOperator(
        task_id='load_to_dds_sales_by_employee',
        postgres_conn_id='target_db',
        database='ecology_analytics',
        sql="""
            truncate dds.sales_by_employee;
            INSERT INTO dds.sales_by_employee (sales_person, invoice_date, total_price)
            SELECT sales_person, invoice_date, SUM(price)
            FROM ods.fact_sale fs2
            JOIN ods.dim_seller ds ON fs2.seller_id = ds.seller_id
            GROUP BY sales_person, invoice_date;
        """
    )

    trigger_dag_operator = TriggerDagRunOperator(
        task_id='trigger_stream_dag',
        trigger_dag_id='Stream',
        dag=dag
    )

    listen_source_db >> extract_from_postgres_ps2 >> [load_to_stg_age_group, load_to_stg_city, load_to_stg_country,
                                                      load_to_stg_customer,
                                                      load_to_stg_invoice_line, load_to_stg_region,
                                                      load_to_stg_region_sline,
                                                      load_to_stg_reservation_line,
                                                      load_to_stg_reservations, load_to_stg_resort, load_to_stg_sales,
                                                      load_to_stg_sales_person,
                                                      load_to_stg_sales_service,
                                                      load_to_stg_sales_service_line] >> load_to_nds >> \
    load_to_ods >> [load_to_dds_sales_with_moving_average, load_to_dds_sales_growth_by_month,
                    load_to_dds_average_sales_by_day_of_week, load_to_dds_sales_trends_by_month,
                    load_to_dds_average_sales_by_seller,
                    load_to_dds_sales_by_service, load_to_dds_customer_age_groups,
                    load_to_dds_sales_by_country_and_region,
                    load_to_dds_sales_by_employee] >> trigger_dag_operator
