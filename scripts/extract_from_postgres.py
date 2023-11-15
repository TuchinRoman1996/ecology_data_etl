import psycopg2
import uuid


def extract_from_postgres_hist(db_params, **kwargs):
    conn = psycopg2.connect(**db_params)
    cursor = conn.cursor()
    nvos_id = kwargs['ti'].xcom_pull(key='nvos_id')

    sql_query = f"""
        select
        n.nvos_id, 
        n.code object_code, 
        n."name" object_name, 
        n."location" object_location, 
        o.organization_name org_name, 
        o.legal_address org_address, 
        o.inn inn, 
        n.category object_category, 
        e.source_count emission_count, 
        e.total_emission emission_total, 
        d.discharge_count discharge_count,
        d.total_discharge discharge_total, 
        w.object_count waste_count, 
        w.total_mass waste_total, 
        g.co2_emission co2_total
        from nvosentities n 
        join organizations o on n.org_id = o.org_id 
        join discharges d ON n.nvos_id = d.nvos_id 
        join emissions e ON n.nvos_id = e.nvos_id 
        join greenhousegases g ON n.nvos_id = g.nvos_id 
        join waste w ON n.nvos_id = w.nvos_id
        """
    if nvos_id:
        sql_query += f'\nwhere n.nvos_id in ({nvos_id})'
    cursor.execute(sql_query)

    data = {'data': cursor.fetchall(), 'col': [desc[0] for desc in cursor.description]}

    cursor.close()
    conn.close()

    kwargs['ti'].xcom_push(key='extracted_data_hist', value=data)

    print(f"Данные извлечены успешно")


def extract_from_postgresql_club_ru(db_params, **kwargs):
    conn = psycopg2.connect(**db_params)
    cursor = conn.cursor()

    cursor.execute("""
      SELECT age_group_id, age_min, age_max, age_range
      FROM public.age_group;
    """)

    data_for_age_group = cursor.fetchall()
    kwargs['ti'].xcom_push(key='data_for_age_group', value=data_for_age_group)

    cursor.execute("""
        SELECT city_id, city, region_id
        FROM public.city;
    """)

    data_for_city = cursor.fetchall()
    kwargs['ti'].xcom_push(key='data_for_city', value=data_for_city)

    cursor.execute("""
        SELECT country_id, country
        FROM public.country;
    """)

    data_for_country = cursor.fetchall()
    kwargs['ti'].xcom_push(key='data_for_country', value=data_for_country)

    cursor.execute("""
        SELECT cust_id, first_name, last_name, age, phone_number, address, city_id, sales_id, sponsor_id
        FROM public.customer;
    """)
    data_for_customer = cursor.fetchall()
    kwargs['ti'].xcom_push(key='data_for_customer', value=data_for_customer)

    cursor.execute("""
        SELECT inv_id, service_id, days, nb_guests
        FROM public.invoice_line;
    """)
    data_for_invoice_line = cursor.fetchall()
    kwargs['ti'].xcom_push(key='data_for_invoice_line', value=data_for_invoice_line)

    cursor.execute("""
        SELECT inv_id, service_id, days, nb_guests
        FROM public.invoice_line;
    """)
    data_for_invoice_line = cursor.fetchall()
    kwargs['ti'].xcom_push(key='data_for_invoice_line', value=data_for_invoice_line)

    cursor.execute("""
        SELECT region_id, region, country_id
        FROM public.region;
    """)
    data_for_region = cursor.fetchall()
    kwargs['ti'].xcom_push(key='data_for_region', value=data_for_region)

    cursor.execute("""
        SELECT sl_id, region_id, sales_revenue
        FROM public.region_sline;
    """)
    data_for_region_sline = cursor.fetchall()
    kwargs['ti'].xcom_push(key='data_for_region_sline', value=data_for_region_sline)

    cursor.execute("""
        SELECT res_id, service_id, res_days, future_guests
        FROM public.reservation_line;
     """)
    data_for_reservation_line = cursor.fetchall()
    kwargs['ti'].xcom_push(key='data_for_reservation_line', value=data_for_reservation_line)

    cursor.execute("""
        SELECT res_id, cust_id, res_date
        FROM public.reservations;
    """)
    data_for_reservation = cursor.fetchall()
    kwargs['ti'].xcom_push(key='data_for_reservation', value=data_for_reservation)

    cursor.execute("""
        SELECT resort_id, resort, country_id
        FROM public.resort;
    """)
    data_for_resort = cursor.fetchall()
    kwargs['ti'].xcom_push(key='data_for_resort', value=data_for_resort)

    cursor.execute("""
        SELECT inv_id, cust_id, invoice_date
        FROM public.sales;
    """)
    data_for_sales = cursor.fetchall()
    kwargs['ti'].xcom_push(key='data_for_sales', value=data_for_sales)

    cursor.execute("""
        SELECT sales_id, sales_person
        FROM public.sales_person;
    """)
    data_for_sales_person = cursor.fetchall()
    kwargs['ti'].xcom_push(key='data_for_sales_person', value=data_for_sales_person)

    cursor.execute("""
        SELECT service_id, service, sl_id, price
        FROM public.service;
    """)
    data_for_service = cursor.fetchall()
    kwargs['ti'].xcom_push(key='data_for_service', value=data_for_service)

    cursor.execute("""
        SELECT sl_id, service_line, resort_id
        FROM public.service_line;
     """)
    data_for_service_line = cursor.fetchall()
    kwargs['ti'].xcom_push(key='data_for_service_line', value=data_for_service_line)

    cursor.close()
    conn.close()