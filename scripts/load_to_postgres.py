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
        INSERT INTO address(id, postcode, region, locality, street, house_number)
        VALUES %s
    """
    insert_into_organizations_sql = """
        INSERT INTO organizations(id, org_name, org_inn)
        VALUES %s
    """

    insert_into_organization_x_address_sql = """
            INSERT INTO organizations_x_address(id, organization_id, address_id)
            VALUES %s
        """

    insert_into_object_categoty_sql = """
            INSERT INTO object_category(id, category_name, category_code, category_num)
            VALUES %s
            ON CONFLICT DO NOTHING;
        """

    insert_into_objects_sql = """
            INSERT INTO objects(id, nvos_id, object_code, object_name, object_location,
            category_id, emission_count, discharge_count, waste_count)
            VALUES %s
        """

    insert_into_environmental_emissions_sql = """
            INSERT INTO environmental_emissions(nvos_id, org_address_id, obj_id, emission_total, discharge_total,
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
