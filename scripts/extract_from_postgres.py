import psycopg2


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
