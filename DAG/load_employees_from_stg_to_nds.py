from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.dates import days_ago

# Определяем стандартный аргументы DAG
default_args = {
    'owner': 'RTuchin',
    'depends_on_past': False,
    'start_date': days_ago(0),
    'catchup': 'false'
}

# Определяем DAG
with DAG('load_employees_from_stg_to_nds', default_args=default_args, schedule_interval=None, catchup=False,
         tags=['test_db']) as dag:

    # Определяем и вставляем новые данные для DWH_DSO_3NDSemployees
    insert_new_data_task = PostgresOperator(
        task_id='insert_new_data',
        postgres_conn_id='test_db',
        sql="""
            INSERT INTO nds."DWH_DSO_3NDSemployees"
            (request_id, record_id, "mode", id_employee, fio, id_region, id_company, l_faktor, j_faktor, x_faktor, birth_date, date_devations, uik, uik_num)
            WITH distinct_request_id AS (
                SELECT DISTINCT request_id 
                FROM nds."DWH_DSO_3NDSemployees" ddn
            ),
            last_request_id AS (
                SELECT *
                FROM stg."DWH_DSO_2STGmetadata" dds 	
                WHERE table_name = 'DWH_DSO_1STGemployees'
                    AND request_id NOT IN (SELECT request_id FROM distinct_request_id)
                ORDER BY load_date 
                LIMIT 1
            ),
            distinct_employee_id_stg as (
                select distinct  id_employee
                from stg."DWH_DSO_1STGemployees" dds 
                join last_request_id lr on dds.request_id = lr.request_id
            ),
            disttinct_employee_id_nds as (
                select distinct id_employee::text
                from nds."DWH_DSO_3NDSemployees" ddn 
                where "mode" != 'DELETE'
            ),
            id_employee_for_insert as (
                select id_employee 
                from distinct_employee_id_stg
                except
                select id_employee 
                from disttinct_employee_id_nds
            )
            select 
                dds2.request_id 
                ,record_id 
                ,'CREATE' as "mode"
                ,dds2.id_employee::bigint
                ,fio::text 
                ,id_region::bigint
                ,id_company::bigint
                ,l_faktor::bigint
                ,j_faktor::bigint
                ,x_faktor::bigint
                ,birth_date::timestamp 
                ,date_devations::timestamp 
                ,uik::text 
                ,uik_num::bigint
            from stg."DWH_DSO_1STGemployees" dds2
            join last_request_id lr on dds2.request_id = lr.request_id
            join id_employee_for_insert ie on dds2.id_employee = ie.id_employee;
        """
    )

    # Определяем и обновляем данные для DWH_DSO_3NDSemployees
    update_data_task = PostgresOperator(
        task_id='update_data',
        postgres_conn_id='test_db',
        sql="""
            INSERT INTO nds."DWH_DSO_3NDSemployees"
            (request_id, record_id, "mode", id_employee, fio, id_region, id_company, l_faktor, j_faktor, x_faktor, birth_date, date_devations, uik, uik_num)
            WITH distinct_request_id AS (
                SELECT DISTINCT request_id 
                FROM nds."DWH_DSO_3NDSemployees" ddn
            ),
            last_request_id AS (
                SELECT *
                FROM stg."DWH_DSO_2STGmetadata" dds 	
                WHERE table_name = 'DWH_DSO_1STGemployees'
                    AND request_id NOT IN (SELECT request_id FROM distinct_request_id)
                ORDER BY load_date 
                LIMIT 1
            ),
            distinct_employee_id_stg as (
                select distinct  id_employee
                from stg."DWH_DSO_1STGemployees" dds 
                join last_request_id lr on dds.request_id = lr.request_id
            ),
            disttinct_employee_id_nds as (
                select distinct id_employee::text
                from nds."DWH_DSO_3NDSemployees" ddn 
                where "mode" != 'DELETE'
            ),
            id_employee_for_insert as (
                select id_employee 
                from distinct_employee_id_stg
                except
                select id_employee 
                from disttinct_employee_id_nds
            )
            select dds2.request_id 
                ,dds2.record_id 
                ,'UPDATE' as "mode"
                ,t1.id_employee 
                ,t1.fio 
                ,t1.id_region 
                ,t1.id_company 
                ,t1.l_faktor 
                ,t1.j_faktor 
                ,t1.x_faktor 
                ,t1.birth_date 
                ,t1.date_devations 
                ,t1.uik 
                ,t1.uik_num 
            from (
            SELECT id_employee::bigint
                ,fio::text
                ,id_region::bigint
                ,id_company::bigint
                ,l_faktor::bigint
                ,j_faktor::bigint
                ,x_faktor::bigint
                ,birth_date::timestamp
                ,date_devations::timestamp
                ,uik::text
                ,uik_num::bigint
            FROM stg."DWH_DSO_1STGemployees" e
            join last_request_id lr on e.request_id = lr.request_id
            except
            select id_employee
                ,fio
                ,id_region
                ,id_company
                ,l_faktor
                ,j_faktor
                ,x_faktor
                ,birth_date
                ,date_devations
                ,uik
                ,uik_num
            FROM nds."DWH_DSO_3NDSemployees")t1
            join stg."DWH_DSO_1STGemployees" dds2 on dds2.id_employee::bigint = t1.id_employee
            join last_request_id lr on dds2.request_id = lr.request_id;
        """
    )

    # Определяем и вставляем новые данные для DWH_AO_TNDSemployees
    insert_into_TNDSemployees_task = PostgresOperator(
        task_id='insert_into_TNDSemployees',
        postgres_conn_id='test_db',
        sql="""
            INSERT INTO nds."DWH_AO_TNDSemployees"
            (id_employee, first_name, last_name, sur_name, birth_date)
            WITH RankedRecords AS (
                SELECT 
                    id_employee::int8,
                    fio::text,
                    birth_date::timestamp,
                    ROW_NUMBER() OVER (PARTITION BY id_employee ORDER BY record_id DESC) AS rn
                FROM nds."DWH_DSO_3NDSemployees"
            )
            SELECT 
                id_employee,
                CASE WHEN fio IS NOT NULL THEN split_part(fio, ' ', 1) ELSE NULL END AS first_name,
                CASE WHEN fio IS NOT NULL THEN split_part(fio, ' ', 2) ELSE NULL END AS last_name,
                CASE WHEN fio IS NOT NULL THEN split_part(fio, ' ', 3) ELSE NULL END AS sur_name,
                birth_date
            FROM RankedRecords
            WHERE rn = 1
        ON CONFLICT (id_employee) DO UPDATE
            SET 
                first_name = EXCLUDED.first_name,
                last_name = EXCLUDED.last_name,
                sur_name = EXCLUDED.sur_name,
                birth_date = EXCLUDED.birth_date;
        """
    )

    # Обновляем данные для  DWH_DSO_2NDSemployees
    insert_into_2NDSemployees_task = PostgresOperator(
        task_id='insert_into_2NDSemployees',
        postgres_conn_id='test_db',
        sql="""
           INSERT INTO nds."DWH_DSO_2NDSemployees"
            (id_employee, id_region, id_company, l_faktor, j_faktor, x_faktor, date_devations, uik, uik_num)
            WITH RankedRecords AS (
                SELECT 
                    id_employee::int8,
                    id_region::int8,
                    id_company::int8,
                    l_faktor::numeric(14),
                    j_faktor::numeric(14),
                    x_faktor::numeric(14),
                    date_devations::timestamp,
                    uik::text,
                    uik_num::numeric(14),
                    ROW_NUMBER() OVER (PARTITION BY id_employee ORDER BY record_id DESC) AS rn
                FROM nds."DWH_DSO_3NDSemployees"
            )
            SELECT 
                id_employee,
                id_region,
                id_company,
                l_faktor,
                j_faktor,
                x_faktor,
                date_devations,
                uik,
                uik_num
            FROM RankedRecords
            WHERE rn = 1
            ON CONFLICT (id_employee) DO UPDATE
            SET 
                id_region = EXCLUDED.id_region,
                id_company = EXCLUDED.id_company,
                l_faktor = EXCLUDED.l_faktor,
                j_faktor = EXCLUDED.j_faktor,
                x_faktor = EXCLUDED.x_faktor,
                date_devations = EXCLUDED.date_devations,
                uik = EXCLUDED.uik,
                uik_num = EXCLUDED.uik_num;
        """
    )

    insert_new_data_task >> insert_into_TNDSemployees_task >> insert_into_2NDSemployees_task
    update_data_task >> insert_into_TNDSemployees_task >> insert_into_2NDSemployees_task
