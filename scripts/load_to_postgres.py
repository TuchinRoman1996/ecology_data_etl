import psycopg2
from psycopg2.extras import execute_values
import pandas as pd


def load_to_postgres_hist_function(db_params, **kwargs):
    conn = psycopg2.connect(**db_params)
    cursor = conn.cursor()

    # Получение преобразованных значений
    address = kwargs['ti'].xcom_pull(key='address')
    organizations = kwargs['ti'].xcom_pull(key='organizations')
    organization_x_address = kwargs['ti'].xcom_pull(key='organization_x_address')
    object_category = kwargs['ti'].xcom_pull(key='object_category')
    objects = kwargs['ti'].xcom_pull(key='objects')
    environmental_emissions = kwargs['ti'].xcom_pull(key='environmental_emissions')

    print(f'После трансформации: {address[0]}')

    # Определение очереди для вставки значений
    address_values = [
        (i[0],
         None if pd.isna(i[1]) else i[1],
         None if pd.isna(i[2]) else i[2],
         None if pd.isna(i[3]) else i[3],
         None if pd.isna(i[4]) else i[4],
         None if pd.isna(i[5]) else i[5]) for i in address
    ]
    organizations_values = [
        (i[0],
         None if pd.isna(i[1]) else i[1],
         i[2]) for i in organizations]
    organization_x_address_values = [(i[0], i[1], i[2]) for i in organization_x_address]
    object_category_values = [(i[1], i[2], i[0], i[3]) for i in object_category]
    objects_values = [(i[1], i[0], i[2], i[3], i[5], i[4], i[6], i[7], i[8]) for i in objects]
    environmental_emissions_values = [
        (i[0], i[1], i[2],
         0 if pd.isna(i[3]) else i[3],
         0 if pd.isna(i[4]) else i[4],
         0 if pd.isna(i[5]) else i[5],
         0 if pd.isna(i[6]) else i[6]) for i in environmental_emissions]

    print(f'Для вставки: {address_values[0]}')

    # Запросы для вставки
    insert_into_address = """
        INSERT INTO public.address(id, postcode, region, locality, street, house_number)
        VALUES %s
    """
    insert_into_organizations_sql = """
        INSERT INTO public.organizations(id, org_name, org_inn)
        VALUES %s
    """

    insert_into_organization_x_address_sql = """
            INSERT INTO public.organizations_x_address(id, organization_id, address_id)
            VALUES %s
        """

    insert_into_object_categoty_sql = """
            INSERT INTO public.object_category(id, category_name, category_code, category_num)
            VALUES %s
            ON CONFLICT DO NOTHING;
        """

    insert_into_objects_sql = """
            INSERT INTO public.objects(id, nvos_id, object_code, object_name, object_location,
            category_id, emission_count, discharge_count, waste_count)
            VALUES %s
        """

    insert_into_environmental_emissions_sql = """
            INSERT INTO public.environmental_emissions(nvos_id, org_address_id, obj_id, emission_total, discharge_total,
            waste_total, co2_total)
            VALUES %s
        """

    # Пакетная вставка данных в таблицы
    execute_values(cursor, insert_into_address, address_values, page_size=10000)
    execute_values(cursor, insert_into_organizations_sql, organizations_values, page_size=10000)
    execute_values(cursor, insert_into_organization_x_address_sql, organization_x_address_values, page_size=10000)
    execute_values(cursor, insert_into_object_categoty_sql, object_category_values, page_size=10000)
    execute_values(cursor, insert_into_objects_sql, objects_values, page_size=10000)
    execute_values(cursor, insert_into_environmental_emissions_sql, environmental_emissions_values, page_size=10000)

    conn.commit()

    cursor.close()
    conn.close()


def load_to_stg_age_group(db_params, **kwargs):
    conn = psycopg2.connect(**db_params)
    cursor = conn.cursor()

    data_for_age_group = kwargs['ti'].xcom_pull(key='data_for_age_group')
    print(data_for_age_group)

    execute_values(cursor, f"""
    INSERT INTO stg."/DWH/AO/0age_group" (request_id, age_group_id, age_min, age_max, age_range)
    VALUES %s
    """, data_for_age_group, page_size=10000)

    conn.commit()

    cursor.close()
    conn.close()


def load_to_stg_city(db_params, **kwargs):
    conn = psycopg2.connect(**db_params)
    cursor = conn.cursor()

    data_for_city = kwargs['ti'].xcom_pull(key='data_for_city')
    print(data_for_city[0])

    execute_values(cursor, f"""
    INSERT INTO stg."/DWH/AO/0city" (request_id, city_id, city, region_id)
    VALUES %s
    """, data_for_city, page_size=10000)

    conn.commit()

    cursor.close()
    conn.close()


def load_to_stg_country(db_params, **kwargs):
    conn = psycopg2.connect(**db_params)
    cursor = conn.cursor()

    data_for_country = kwargs['ti'].xcom_pull(key='data_for_country')
    print(data_for_country[0])

    execute_values(cursor, f"""
    INSERT INTO stg."/DWH/AO/0country" (request_id, country_id, country)
    VALUES %s
    """, data_for_country, page_size=10000)

    conn.commit()

    cursor.close()
    conn.close()


def load_to_stg_customer(db_params, **kwargs):
    conn = psycopg2.connect(**db_params)
    cursor = conn.cursor()

    data_for_customer = kwargs['ti'].xcom_pull(key='data_for_customer')
    print(data_for_customer[0])

    execute_values(cursor, f"""
    INSERT INTO stg."/DWH/AO/0customer" (request_id, cust_id, first_name, last_name, age, phone_number, address, city_id, 
    sales_id, sponsor_id)
    VALUES %s
    """, data_for_customer, page_size=10000)

    conn.commit()

    cursor.close()
    conn.close()


def load_to_stg_invoice_line(db_params, **kwargs):
    conn = psycopg2.connect(**db_params)
    cursor = conn.cursor()

    data_for_invoice_line = kwargs['ti'].xcom_pull(key='data_for_invoice_line')
    print(data_for_invoice_line[0])

    execute_values(cursor, f"""
    INSERT INTO stg."/DWH/DSO/3STGinvoice_line" (request_id, inv_id, service_id, days, nb_guests)
    VALUES %s
    """, data_for_invoice_line, page_size=10000)

    conn.commit()

    cursor.close()
    conn.close()


def load_to_stg_region(db_params, **kwargs):
    conn = psycopg2.connect(**db_params)
    cursor = conn.cursor()

    data_for_region = kwargs['ti'].xcom_pull(key='data_for_region')
    print(data_for_region[0])

    execute_values(cursor, f"""
    INSERT INTO stg."/DWH/AO/0region" (request_id, region_id, region, country_id)
    VALUES %s
    """, data_for_region, page_size=10000)

    conn.commit()

    cursor.close()
    conn.close()


def load_to_stg_region_sline(db_params, **kwargs):
    conn = psycopg2.connect(**db_params)
    cursor = conn.cursor()

    data_for_region_sline = kwargs['ti'].xcom_pull(key='data_for_region_sline')
    print(data_for_region_sline[0])

    execute_values(cursor, f"""
    INSERT INTO stg."/DWH/DSO/3STGregion_sline" (request_id, sl_id, region_id, sales_revenue)
    VALUES %s
    """, data_for_region_sline, page_size=10000)

    conn.commit()

    cursor.close()
    conn.close()


def load_to_stg_reservation_line(db_params, **kwargs):
    conn = psycopg2.connect(**db_params)
    cursor = conn.cursor()

    data_for_reservation_line = kwargs['ti'].xcom_pull(key='data_for_reservation_line')
    print(data_for_reservation_line[0])

    execute_values(cursor, f"""
    INSERT INTO stg."/DWH/DSO/3STGreservation_line" (request_id, res_id, service_id, res_days, future_guests)
    VALUES %s
    """, data_for_reservation_line, page_size=10000)

    conn.commit()

    cursor.close()
    conn.close()


def load_to_stg_reservations(db_params, **kwargs):
    conn = psycopg2.connect(**db_params)
    cursor = conn.cursor()

    data_for_reservation = kwargs['ti'].xcom_pull(key='data_for_reservation')
    print(data_for_reservation[0])

    execute_values(cursor, f"""
    INSERT INTO stg."/DWH/DSO/3STGreservations" (request_id, res_id, cust_id, res_date)
    VALUES %s
    """, data_for_reservation, page_size=10000)

    conn.commit()

    cursor.close()
    conn.close()


def load_to_stg_resort(db_params, **kwargs):
    conn = psycopg2.connect(**db_params)
    cursor = conn.cursor()

    data_for_resort = kwargs['ti'].xcom_pull(key='data_for_resort')
    print(data_for_resort[0])

    execute_values(cursor, f"""
    INSERT INTO stg."/DWH/AO/0resort" (request_id, resort_id, resort, country_id)
    VALUES %s
    """, data_for_resort, page_size=10000)

    conn.commit()

    cursor.close()
    conn.close()


def load_to_stg_sales(db_params, **kwargs):
    conn = psycopg2.connect(**db_params)
    cursor = conn.cursor()

    data_for_sales = kwargs['ti'].xcom_pull(key='data_for_sales')
    print(data_for_sales[0])

    execute_values(cursor, f"""
    INSERT INTO stg."/DWH/DSO/3STGsales" (request_id, inv_id, cust_id, invoice_date)
    VALUES %s
    """, data_for_sales, page_size=10000)

    conn.commit()

    cursor.close()
    conn.close()


def load_to_stg_sales_person(db_params, **kwargs):
    conn = psycopg2.connect(**db_params)
    cursor = conn.cursor()

    data_for_sales_person = kwargs['ti'].xcom_pull(key='data_for_sales_person')
    print(data_for_sales_person[0])

    execute_values(cursor, f"""
    INSERT INTO stg."/DWH/AO/0sales_person" (request_id, sales_id, sales_person)
    VALUES %s
    """, data_for_sales_person, page_size=10000)

    conn.commit()

    cursor.close()
    conn.close()


def load_to_stg_sales_service(db_params, **kwargs):
    conn = psycopg2.connect(**db_params)
    cursor = conn.cursor()

    data_for_service = kwargs['ti'].xcom_pull(key='data_for_service')
    print(data_for_service[0])

    execute_values(cursor, f"""
    INSERT INTO stg."/DWH/AO/0service" (request_id, service_id, service, sl_id, price)
    VALUES %s
    """, data_for_service, page_size=10000)

    conn.commit()

    cursor.close()
    conn.close()


def load_to_stg_sales_service_line(db_params, **kwargs):
    conn = psycopg2.connect(**db_params)
    cursor = conn.cursor()

    data_for_service_line = kwargs['ti'].xcom_pull(key='data_for_service_line')
    print(data_for_service_line[0])

    execute_values(cursor, f"""
    INSERT INTO stg."/DWH/DSO/3STGservice_line" (request_id, sl_id, service_line, resort_id)
    VALUES %s
    """, data_for_service_line, page_size=10000)

    conn.commit()

    cursor.close()
    conn.close()