import pandas as pd
from decimal import Decimal
import re


def string_to_decimal(value):
    if value:
        cleaned_value = re.sub(r'\s', '', value).replace(',', '.')
        decimal_value = Decimal(cleaned_value)
        return decimal_value


def transform_data_function_h(db_params, **kwargs):
    data = kwargs['ti'].xcom_pull(key='extracted_data_hist')
    nvos_id = kwargs['ti'].xcom_pull(key='nvos_id')
    df = pd.DataFrame(data['data'], columns=data['col'])

    def recode_values(x, mapping):
        return mapping.get(x, None)

    # Подготовка данных для таблицы address
    df_address = pd.DataFrame()
    df_address['nvos_id'] = df['nvos_id']
    df_address['postcode'] = df['org_address'].str.extract(r'(\d{5,6})')
    df_address['region'] = df['org_address'].str.extract(r',\s*([А-Яа-я]*\s(край|обл|область))')[0]
    df_address['locality'] = df['org_address'].str.extract(
        r',\s*((?:г\.|г|город|с\.|c|село|п\.|п|пос(е|ё)лок|д\.|д.|деревня)\s*[А-Яа-я-]*)\s*,')[0]
    df_address['street'] = df['org_address'].str.extract(r',\s*((пр-кт|ул\.|ул|улица|пр-д)\s*[А-Яа-я]*),?|,\s*(['
                                                         r'А-Яа-я]*\s*(улица|ул\.|ул)),?')[0]
    df_address['house_number'] = \
    df['org_address'].str.extract(r',\s*((д|д\.|дом)\s*\d{1,4}[а-я]*)', flags=re.IGNORECASE)[0]

    df_address['id'] = df_address[['postcode', 'region', 'locality', 'street', 'house_number']].apply(tuple, axis=1).astype('category').cat.codes + (100000 if nvos_id else 1)

    df_address_deduplicate = df_address[['id',
                                         'postcode',
                                         'region',
                                         'locality',
                                         'street',
                                         'house_number']].drop_duplicates()

    address = [tuple(x) for x in df_address_deduplicate.itertuples(index=False, name=None)]
    kwargs['ti'].xcom_push(key='address', value=address)

    # Подготовка данных для таблицы organization
    df_organization = pd.DataFrame()
    df_organization['nvos_id'] = df['nvos_id']
    df_organization['org_name'] = df['org_name'].str.extract(r'(?:"|`)(.*)(?:`|")')[0]
    df_organization['inn'] = df['inn'].str.replace(r'\D', '', regex=True)
    df_organization['id'] = df_organization[['org_name', 'inn']].apply(tuple, axis=1).astype('category').cat.codes + (
        100000 if nvos_id else 1)

    df_organization = df_organization.replace('', None)

    df_organization_deduplicate = df_organization[['id',
                                                   'org_name',
                                                   'inn']].drop_duplicates()

    organizations = [tuple(x) for x in df_organization_deduplicate.itertuples(index=False, name=None)]
    kwargs['ti'].xcom_push(key='organizations', value=organizations)

    # Подготовка данных для таблицы organization_x_address
    df_organization_x_address = pd.DataFrame()
    df_organization_x_address['nvos_id'] = df['nvos_id']
    df_organization_x_address['org_id'] = df_organization_x_address['nvos_id'].map(
        df_organization.set_index('nvos_id')['id'])
    df_organization_x_address['address_id'] = df_organization_x_address['nvos_id'].map(
        df_address.set_index('nvos_id')['id'])
    df_organization_x_address['id'] = df_organization_x_address[['org_id', 'address_id']].apply(tuple, axis=1).astype('category').cat.codes + (
        100000 if nvos_id else 1)

    df_organization_x_address_uniq = df_organization_x_address[[
            'id',
            'org_id',
            'address_id'
    ]].drop_duplicates()

    organizations_x_address = [tuple(x) for x in df_organization_x_address_uniq.itertuples(index=False, name=None)]
    kwargs['ti'].xcom_push(key='organization_x_address', value=organizations_x_address)

    # Подготовка данных для таблицы objects_category
    df_object_category = pd.DataFrame()
    df_object_category['category_code'] = df['object_category'].drop_duplicates()
    df_object_category['id'] = df_object_category.index * 12 + 1

    category_mapping = {
        'I': {'category_name': 'Первая категория', 'category_num': 1},
        'II': {'category_name': 'Вторая категория', 'category_num': 2},
        'III': {'category_name': 'Третья категория', 'category_num': 3},
        'IV': {'category_name': 'Четвёртая категория', 'category_num': 4},
    }

    df_object_category['category_name'] = df_object_category['category_code'].apply(
        lambda x: recode_values(x, category_mapping)['category_name'])
    df_object_category['category_num'] = df_object_category['category_code'].apply(
        lambda x: recode_values(x, category_mapping)['category_num'])

    object_category = [tuple(x) for x in df_object_category.itertuples(index=False, name=None)]
    kwargs['ti'].xcom_push(key='object_category', value=object_category)

    # Подготовка данных для таблицы objects
    df_objects = pd.DataFrame()
    df_objects['nvos_id'] = df['nvos_id']
    df_objects['id'] = df_objects['nvos_id'] * 9
    df_objects['object_code'] = df['object_code']
    df_objects['object_name'] = df['object_name'].str.replace(r'\s+', ' ')
    df_objects['category_id'] = df['object_category'].map(df_object_category.set_index('category_code')['id'])
    df_objects['object_location'] = df['object_location'].str.replace(r'\s+', ' ')
    df_objects['emission_count'] = df['emission_count']
    df_objects['discharge_count'] = df['discharge_count']
    df_objects['waste_count'] = df['waste_count']
    df_objects = df_objects.replace('', None)

    objects = [tuple(x) for x in df_objects.itertuples(index=False, name=None)]
    kwargs['ti'].xcom_push(key='objects', value=objects)

    # Подготовка данных для таблицы environmental_emissions
    df_environmental_emissions = pd.DataFrame()
    df_environmental_emissions['nvos_id'] = df['nvos_id']
    df_environmental_emissions['org_address_id'] = df['nvos_id'].map(
        df_organization_x_address.set_index('nvos_id')['id'])
    df_environmental_emissions['obj_id'] = df['nvos_id'].map(df_objects.set_index('nvos_id')['id'])
    df_environmental_emissions['emission_total'] = df['emission_total'].apply(string_to_decimal).astype(float)
    df_environmental_emissions['discharge_total'] = df['discharge_total'].apply(string_to_decimal).astype(float)
    df_environmental_emissions['waste_total'] = df['waste_total'].apply(string_to_decimal).astype(float)
    df_environmental_emissions['co2_total'] = df['co2_total'].apply(string_to_decimal).astype(float)

    environmental_emissions = [tuple(x) for x in df_environmental_emissions.itertuples(index=False, name=None)]
    kwargs['ti'].xcom_push(key='environmental_emissions', value=environmental_emissions)

    print('Трансформация прошла успешно')
