
from airflow import DAG
#from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
#from airflow.providers.postgres.hooks.postgres import PostgresHook
import pendulum


#Базовые переменные
POSTGRES_CONN_ID = 'PG_WAREHOUSE_CONNECTION'

with DAG (
    'dds_dag',
    schedule_interval='0/15 * * * *',  # Задаем расписание выполнения дага - каждый 15 минут.
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),  # Дата начала выполнения дага. Можно поставить сегодня.
    catchup=False,  # Нужно ли запускать даг за предыдущие периоды (с start_date до сегодня) - False (не нужно).
    tags=['sprint5', 'dds', 'origin'],  # Теги, используются для фильтрации в интерфейсе Airflow.
    is_paused_upon_creation=True  # Остановлен/запущен при появлении. Сразу запущен.

) as dag:
        
    dm_restaurant = PostgresOperator(
    task_id='refresh_dm_restaurants',
    postgres_conn_id=POSTGRES_CONN_ID,
    sql="sql/dm_restaurants.sql")

    dm_timestamps = PostgresOperator (
    task_id='refresh_dm_timestamps',
    postgres_conn_id=POSTGRES_CONN_ID,
    sql="sql/dm_timestamps.sql")

    dm_products = PostgresOperator(
    task_id='refresh_dm_products',
    postgres_conn_id=POSTGRES_CONN_ID,
    sql="sql/dm_products.sql")

    dm_orders = PostgresOperator(
    task_id='refresh_dm_orders',
    postgres_conn_id=POSTGRES_CONN_ID,
    sql="sql/dm_orders.sql")

    fct_product_sales = PostgresOperator(
    task_id='refresh_fct_product_sales',
    postgres_conn_id=POSTGRES_CONN_ID,
    sql="sql/fct_product_sales.sql")

    dm_settlement_report = PostgresOperator(
    task_id='refresh_dm_settlement_report',
    postgres_conn_id=POSTGRES_CONN_ID,
    sql="sql/dm_settlement_report.sql")

    (
    dm_restaurant >> dm_timestamps >> dm_products >> dm_orders >> fct_product_sales >> dm_settlement_report 
    )