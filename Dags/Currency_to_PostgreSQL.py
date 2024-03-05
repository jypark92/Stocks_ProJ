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
    create_table_sql = f"""CREATE TABLE IF NOT EXISTS {schema}.{table} (
                               currency varchar(7) not null,
                               symbol varchar(6) not null
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
            currency, symbol = row["currency"], row["symbol"]
            sql = f"INSERT INTO {schema}.{table} VALUES ('{currency}', '{symbol}');"
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
    dag_id='currency_to_postgresql',
    start_date=datetime(2022, 10, 6),  # 날짜가 미래인 경우 실행이 안됨
    schedule = '@once',  # 적당히 조절
    catchup=False,
) as dag:

    path = Variable.get("currency_path")
    schema = 'raw_data'   ## 자신의 스키마로 변경
    table = 'currency'

    create_table_and_load_data(schema, table, path)