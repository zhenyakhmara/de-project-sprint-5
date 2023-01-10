
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
from datetime import date
import pendulum 
import requests
import json
import pandas as pd
import logging

task_logger = logging.getLogger('airflow.task')


#Базовые переменные
POSTGRES_CONN_ID = 'PG_WAREHOUSE_CONNECTION'
NICKNAME = 'evgeniy-khmara'
COHORT = '7'
API_KEY = '25c27781-8fde-4b30-a22e-524044a7580f'

postgres_hook = PostgresHook(POSTGRES_CONN_ID)
engine = postgres_hook.get_sqlalchemy_engine()
con = engine.connect()
business_dt = '{{ ds }}'

headers = {
    'X-Nickname': NICKNAME,
    'X-Cohort': COHORT,
    'X-API-KEY': API_KEY
}

# Функция загрузки таблицы курьеров
def load_couriers(pg_table, pg_schema):
    limit = 50
    offset = 0
    response_len = 21
    id_column_source = '_id'
    id_column_target = 'courier_id'
    con.execute (f"""truncate table {pg_schema}.{pg_table}""") # очищаем таблицу перед заливкой свежих данных
    while response_len > 20:
        url = f'https://d5d04q7d963eapoepsqr.apigw.yandexcloud.net/couriers?sort_field=name&sort_direction=asc&limit={limit}&offset={offset}'
        r = requests.get (url, headers=headers)
        offset += 50
        response_len = len(r.text)
        if response_len < 20:
            task_logger.info('empty response')
            break
        df1 = pd.read_json(r.text, orient='values')
        df1 = df1[id_column_source].to_frame()
        list_obj = json.loads(r.text)# создаем список объектов
        df1.insert(1,"object_value",list_obj) # добавляем столбец с объектами
        df1.insert (2,"load_dttm",datetime.now()) # добавляем столбец с меткой времени
        df1['object_value'] = list(map(lambda x: json.dumps(x,ensure_ascii =False), df1['object_value'] )) # Переводим в текст
        df1.rename(columns = {id_column_source:id_column_target}, inplace = True )
        df1.to_sql(pg_table, engine, schema=pg_schema, if_exists='append', index=False)

# Функция загрузки таблицы доставки
def load_deliveries (pg_table, pg_schema, date):
    limit = 50
    offset = 0
    response_len = 21
    id_column_source = 'delivery_id'
    id_column_target = 'delivery_id'
   
   # Удаляем записи за сегодня, чтобы их перегрузить в случае ошибки
    date_for_delete = "'" + (date +' 00:00:00') + "'"
    con.execute (f"""delete from  {pg_schema}.{pg_table} where date_trunc ('day',delivery_ts) = {date_for_delete}  """) # очищаем таблицу перед заливкой свежих данных
    
    while response_len > 20:
        url = f"""https://d5d04q7d963eapoepsqr.apigw.yandexcloud.net/deliveries?from={(date +' 00:00:00')}&to={(date+' 23:59:59')}&sort_field=_id&sort_direction=asc&limit={limit}&offset={offset}"""
        r = requests.get (url, headers=headers)
        offset += 50
        response_len = len(r.text)
        if response_len < 20:
            task_logger.info('empty response')
            break
        df1 = pd.read_json(r.text, orient='values')
        df1 = df1[[id_column_source,'delivery_ts']]
        list_obj = json.loads(r.text)# создаем список объектов
        df1.insert(1,"object_value",list_obj) # добавляем столбец с объектами
        df1.insert (2,"load_dttm",datetime.now()) # добавляем столбец с меткой времени
        df1['object_value'] = list(map(lambda x: json.dumps(x,ensure_ascii =False), df1['object_value'] )) # Переводим в текст
        df1.rename(columns = {id_column_source:id_column_target}, inplace = True )
        df1.to_sql(pg_table, engine, schema=pg_schema, if_exists='append', index=False)
       
       
with DAG (
    'dag_project',
    schedule_interval='0/15 * * * *',# Задаем расписание выполнения дага - каждый 15 минут.
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),  # Дата начала выполнения дага.
    catchup=False,  # Нужно ли запускать даг за предыдущие периоды (с start_date до сегодня) - False (не нужно).
    tags=['sprint5_progect', 'stg', 'dds', 'origin'],  # Теги, используются для фильтрации в интерфейсе Airflow.
    is_paused_upon_creation=False # Остановлен/запущен при появлении.

) as dag:
        
    couriers = PythonOperator(
        task_id='deliverysystem_couriers',
        python_callable=load_couriers,
        op_kwargs={'pg_table': 'deliverysystem_couriers',
                   'pg_schema': 'stg'})

    deliveries = PythonOperator(
        task_id='deliverysystem_deliveries',
        python_callable=load_deliveries,
        op_kwargs={'pg_table': 'deliverysystem_deliveries',
                   'pg_schema': 'stg',
                   'date':business_dt})
    
    dm_couriers = PostgresOperator(
    task_id='refresh_dm_couriers',
    postgres_conn_id=POSTGRES_CONN_ID,
    sql="sql/dm_couriers.sql")

    dm_timestamps = PostgresOperator(
    task_id='refresh_dm_timestamps',
    postgres_conn_id=POSTGRES_CONN_ID,
    sql="sql/dm_timestamps_project.sql",
    parameters={"date": {business_dt}})

    fct_deliveries = PostgresOperator(
    task_id='refresh_fct_deliveries',
    postgres_conn_id=POSTGRES_CONN_ID,
    sql="sql/fct_deliveries.sql",
    parameters={"date": {business_dt}})

    dm_courier_ledger = PostgresOperator(
    task_id='refresh_dm_courier_ledger',
    postgres_conn_id=POSTGRES_CONN_ID,
    sql="sql/dm_courier_ledger.sql",
    parameters={"date": {business_dt}})
    
    (
    [couriers, deliveries] >> dm_couriers >> dm_timestamps >> fct_deliveries >> dm_courier_ledger
    )
