import sys
from os.path import abspath, dirname
import uuid

from airflow.providers.postgres.hooks.postgres import PostgresHook

sys.path.append(dirname(dirname(abspath(__file__))))

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator
from airflow.models import Connection

from scripts.extract_from_postgres import extract_from_postgresql_club_ru
from scripts.load_to_postgres import load_to_stg_age_group, load_to_stg_city, \
    load_to_stg_country, load_to_stg_customer, load_to_stg_invoice_line, load_to_stg_region, \
    load_to_stg_reservation_line, load_to_stg_reservations, load_to_stg_resort, load_to_stg_sales, \
    load_to_stg_sales_person, load_to_stg_sales_service, load_to_stg_sales_service_line, load_to_stg_region_sline

source_db_conn = Connection.get_connection_from_secrets('club_ru')
source_db_params = {
    'database': source_db_conn.extra_dejson.get('dbname'),
    'user': source_db_conn.login,
    'password': source_db_conn.password,
    'host': source_db_conn.host,
    'port': source_db_conn.port,
}

target_db_conn = Connection.get_connection_from_secrets('target_db')
target_db_params = {
    'database': target_db_conn.extra_dejson.get('dbname'),
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


def load_metadata_to_staging(**kwargs):
    pg_hook = PostgresHook(postgres_conn_id='target_db')

    table_names = ["DWH/DSO/3STGmetadata",
                   "/DWH/AO/0age_group",
                   "/DWH/AO/0city",
                   "/DWH/AO/0country",
                   "/DWH/AO/0customer",
                   "/DWH/DSO/3STGinvoice_line",
                   "/DWH/AO/0region",
                   "/DWH/DSO/3STGregion_sline",
                   "/DWH/DSO/3STGreservation_line",
                   "/DWH/DSO/3STGreservations",
                   "/DWH/AO/0resort",
                   "/DWH/DSO/3STGsales",
                   "/DWH/AO/0sales_person",
                   "/DWH/AO/0service",
                   "/DWH/DSO/3STGservice_line"
                   ]

    for table_name in table_names:
        request_id = str(uuid.uuid4())
        kwargs['ti'].xcom_push(key=f'request_id_{table_name}', value=request_id)

        pg_hook.run(f"""
            INSERT INTO stg."DWH/DSO/3STGmetadata"(request_id, status, table_name)
            VALUES('{request_id}', 'CREATE', '{table_name}')
        """)


with DAG('History_2', default_args=default_args, schedule_interval='@once', schedule=None, concurrency=16,
         tags=['club_ru']) as dag:
    extract_from_postgres_ps2 = PythonOperator(
        task_id='extract_from_postgres_ps2',
        python_callable=extract_from_postgresql_club_ru,
        provide_context=True,
        op_args=[source_db_params]
    )

    load_metadata_to_staging_task = PythonOperator(
        task_id='load_metadata_to_staging',
        python_callable=load_metadata_to_staging,
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

    load_dim_resort = PostgresOperator(
        task_id='load_dim_resort',
        postgres_conn_id='target_db',
        database='ecology_analytics',
        sql="select nds.insert_dim_resort();"
    )

    load_dim_seller = PostgresOperator(
        task_id='load_dim_seller',
        postgres_conn_id='target_db',
        database='ecology_analytics',
        sql="select nds.insert_dim_seller();"
    )

    load_dim_service = PostgresOperator(
        task_id='load_dim_service',
        postgres_conn_id='target_db',
        database='ecology_analytics',
        sql="select nds.insert_dim_service();"
    )

    load_dim_country = PostgresOperator(
        task_id='load_dim_country',
        postgres_conn_id='target_db',
        database='ecology_analytics',
        sql="select nds.insert_dim_country();"
    )

    load_dim_service_line = PostgresOperator(
        task_id='load_dim_service_line',
        postgres_conn_id='target_db',
        database='ecology_analytics',
        sql="select nds.insert_dim_service_line();"
    )

    load_dim_customer = PostgresOperator(
        task_id='load_dim_customer',
        postgres_conn_id='target_db',
        database='ecology_analytics',
        sql="select nds.insert_dim_customer();"
    )

    load_fact_sale_data = PostgresOperator(
        task_id='load_fact_sale_data',
        postgres_conn_id='target_db',
        database='ecology_analytics',
        sql="select nds.insert_fact_sale_data();"
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
                FROM nds.fact_sale fs
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
                FROM nds.fact_sale fs
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
                WHEN EXTRACT(DOW FROM fs.invoice_date) = 0 THEN 'Воскресенье'
                WHEN EXTRACT(DOW FROM fs.invoice_date) = 1 THEN 'Понедельник'
                WHEN EXTRACT(DOW FROM fs.invoice_date) = 2 THEN 'Вторник'
                WHEN EXTRACT(DOW FROM fs.invoice_date) = 3 THEN 'Среда'
                WHEN EXTRACT(DOW FROM fs.invoice_date) = 4 THEN 'Четверг'
                WHEN EXTRACT(DOW FROM fs.invoice_date) = 5 THEN 'Пятница'
                WHEN EXTRACT(DOW FROM fs.invoice_date) = 6 THEN 'Суббота'
                ELSE 'Неизвестный день'
              END AS day_of_week,
              AVG(fs.price) AS average_sale
            FROM nds.fact_sale fs
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
            FROM nds.fact_sale fs
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
            FROM nds.fact_sale fs
            JOIN nds.dim_seller ds ON fs.seller_id = ds.seller_id
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
            FROM nds.fact_sale fs
            JOIN nds.dim_service ds ON fs.service_id = ds.service_id
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
                WHEN dc.age BETWEEN 18 AND 29 THEN '18-29'
                WHEN dc.age BETWEEN 30 AND 39 THEN '30-39'
                WHEN dc.age BETWEEN 40 AND 49 THEN '40-49'
                WHEN dc.age BETWEEN 50 AND 59 THEN '50-59'
                WHEN dc.age BETWEEN 60 AND 80 THEN '60-80'
                ELSE 'Unknown'
            END AS age_group,
            COUNT(*) AS customer_count
        FROM nds.dim_customer dc
        WHERE dc.age BETWEEN 18 AND 80
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
            FROM nds.fact_sale fs
            JOIN nds.dim_country dc ON fs.country_id = dc.country_id
            JOIN nds.dim_resort dr ON fs.resort_id = dr.resort_id
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
            FROM nds.fact_sale fs2
            JOIN nds.dim_seller ds ON fs2.seller_id = ds.seller_id
            GROUP BY sales_person, invoice_date;
        """
    )

    extract_from_postgres_ps2 >> load_metadata_to_staging_task >> load_to_stg_age_group
    extract_from_postgres_ps2 >> load_metadata_to_staging_task >> load_to_stg_city
    extract_from_postgres_ps2 >> load_metadata_to_staging_task >> load_to_stg_country >> load_dim_country >> load_fact_sale_data
    extract_from_postgres_ps2 >> load_metadata_to_staging_task >> load_to_stg_customer >> load_dim_customer >> load_fact_sale_data
    extract_from_postgres_ps2 >> load_metadata_to_staging_task >> load_to_stg_invoice_line >> load_fact_sale_data
    extract_from_postgres_ps2 >> load_metadata_to_staging_task >> load_to_stg_region
    extract_from_postgres_ps2 >> load_metadata_to_staging_task >> load_to_stg_region_sline
    extract_from_postgres_ps2 >> load_metadata_to_staging_task >> load_to_stg_reservation_line
    extract_from_postgres_ps2 >> load_metadata_to_staging_task >> load_to_stg_reservations
    extract_from_postgres_ps2 >> load_metadata_to_staging_task >> load_to_stg_resort >> load_dim_resort >> load_fact_sale_data
    extract_from_postgres_ps2 >> load_metadata_to_staging_task >> load_to_stg_sales
    extract_from_postgres_ps2 >> load_metadata_to_staging_task >> load_to_stg_sales_person >> load_dim_seller >> load_fact_sale_data
    extract_from_postgres_ps2 >> load_metadata_to_staging_task >> load_to_stg_sales_service >> load_dim_service >> load_fact_sale_data
    extract_from_postgres_ps2 >> load_metadata_to_staging_task >> load_to_stg_sales_service_line >> load_dim_service_line >> load_fact_sale_data
    load_fact_sale_data >> load_to_dds_sales_with_moving_average
    load_fact_sale_data >> load_to_dds_sales_growth_by_month
    load_fact_sale_data >> load_to_dds_average_sales_by_day_of_week
    load_fact_sale_data >> load_to_dds_sales_trends_by_month
    load_fact_sale_data >> load_to_dds_average_sales_by_seller
    load_fact_sale_data >> load_to_dds_sales_by_service
    load_fact_sale_data >> load_to_dds_customer_age_groups
    load_fact_sale_data >> load_to_dds_sales_by_country_and_region
    load_fact_sale_data >> load_to_dds_sales_by_employee
