from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.dates import days_ago

# Определяем стандартный аргументы DAG
default_args = {
    'owner': 'bi_master',
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
   			with request_id_for_queue as (
            select t1.request_id 
            from stg."DWH_DSO_2STGmetadata" dds 
            join (
                select request_id 
                from stg."DWH_DSO_1STGemployees" dds 
                except
                select request_id 
                from nds."DWH_DSO_3NDSemployees" ddn) t1 on dds.request_id = t1.request_id
            where status = 'SUCCESS'
            order by load_date asc
            limit 1
            ),
            id_employee as (
                select id_employee::bigint
                from stg."DWH_DSO_1STGemployees" dds2 
                join request_id_for_queue ri on dds2.request_id = ri.request_id
                except
                select id_employee
                from nds."DWH_DSO_3NDSemployees" 
                where mode !='DELETE'
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
            join request_id_for_queue ri on dds2.request_id = ri.request_id
            join id_employee ie on dds2.id_employee = ie.id_employee::text;
        """
    )

    # Определяем и обновляем данные для DWH_DSO_3NDSemployees
    update_data_task = PostgresOperator(
        task_id='update_data',
        postgres_conn_id='test_db',
        sql="""
            INSERT INTO nds."DWH_DSO_3NDSemployees"
            (request_id, record_id, "mode", id_employee, fio, id_region, id_company, l_faktor, j_faktor, x_faktor, birth_date, date_devations, uik, uik_num)
            with request_id_for_queue as (
            select t1.request_id 
            from stg."DWH_DSO_2STGmetadata" dds 
            join (
                select request_id 
                from stg."DWH_DSO_1STGemployees" dds 
                except
                select request_id 
                from nds."DWH_DSO_3NDSemployees" ddn) t1 on dds.request_id = t1.request_id
            where status = 'SUCCESS'
            order by load_date asc
            limit 1
        ),
        id_employee as (
            select id_employee::bigint
            from stg."DWH_DSO_1STGemployees" dds2 
            join request_id_for_queue ri on dds2.request_id = ri.request_id
            intersect 
            select id_employee
            from nds."DWH_DSO_3NDSemployees" 
            where mode !='DELETE'
        )
        select 
            dds2.request_id 
            ,record_id
            ,'UPDATE' as "mode"
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
        join request_id_for_queue ri on dds2.request_id = ri.request_id
        join id_employee ie on dds2.id_employee = ie.id_employee::text;
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
                ROW_NUMBER() OVER (PARTITION BY id_employee ORDER BY date_devations DESC) AS rn
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
                    ROW_NUMBER() OVER (PARTITION BY id_employee ORDER BY date_devations DESC) AS rn
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
