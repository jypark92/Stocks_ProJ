from airflow import DAG
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.decorators import task

from datetime import datetime
import pandas as pd
import logging


def get_PostgreSQL_connection():
    hook = PostgresHook(postgres_conn_id='postgresql_dev_db')
    return hook.get_conn()

@task
def create_table_and_load_data(schema, table, path):
    # Create CREATE TABLE SQL statement
    create_table_sql = f"""DROP TABLE IF EXISTS {schema}.{table};
                        CREATE TABLE  {schema}.{table} (
                               stocks_item varchar(32) not null,
                               ticker_symbol varchar(6) not null
                           );"""

    # Execute CREATE TABLE SQL statement
    conn = get_PostgreSQL_connection()
    cur = conn.cursor()
    try:
        cur.execute(create_table_sql)
        cur.execute("COMMIT;")
    except Exception as e:
        cur.execute("ROLLBACK;")
        raise
    logging.info("Create table")

    # Load file into Pandas DataFrame
    df = pd.read_parquet(path)
    logging.info("Load file into Pandas DataFrame")

    # Insert data into PostgreSQL table
    try:
        for _, row in df.iterrows():
            stocks_item, ticker_symbol = row["stocks_item"], row["ticker_symbol"]
            sql = f"INSERT INTO {schema}.{table} VALUES ('{stocks_item}', '{ticker_symbol}');"
            cur.execute(sql)
        cur.execute("COMMIT;")
        logging.info("Data loaded into table successfully")
    except Exception as e:
        cur.execute("ROLLBACK;")
        logging.error(f"Error loading data into table: {e}")
        raise
    finally:
        conn.close()
    logging.info("Load data into table")

with DAG(
    dag_id='stocks_items_to_postgresql',
    start_date=datetime(2022, 10, 6),  # 날짜가 미래인 경우 실행이 안됨
    schedule = '@once',  # 적당히 조절
    catchup=False,
) as dag:

    path = Variable.get("stocks_items_path")
    schema = 'raw_data'   ## 자신의 스키마로 변경
    table = 'stocks_items'

    create_table_and_load_data(schema, table, path)