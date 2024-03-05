from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.decorators import task

from datetime import datetime, timedelta, timezone

import logging
import pandas as pd
from io import BytesIO

def get_PostgreSQL_connection():
    # autocommit is False by default
    hook = PostgresHook(postgres_conn_id='postgresql_dev_db')
    return hook.get_conn()

@task
def extract(start_date, end_date):
    start_key = f'exchange_rate/year={start_date[:4]}/month={start_date[5:7]}/day={start_date[8:10]}/result.parquet'
    end_key = f'exchange_rate/year={end_date[:4]}/month={end_date[5:7]}/day={end_date[8:10]}/result.parquet'

    df_list = []
    s3_hook = S3Hook(aws_conn_id='aws_conn_id')  # Airflow Connection에 정의된 AWS 연결 ID
    # S3 버킷 이름과 객체 키 설정
    bucket_name = 'de-1-1-bucket'
    keys = s3_hook.list_keys(bucket_name, prefix='exchange_rate')
    for key in keys:
        if start_key <= key <= end_key:
            response = s3_hook.get_key(key, bucket_name).get()
            content = response['Body'].read()
            with BytesIO(content) as f:
                df_list.append(pd.read_parquet(f))
    records = pd.concat(df_list).reset_index(drop=True).to_dict()
    return records

@task
def transform(records):
    df = pd.DataFrame(records)
    df["date"] = df["date"].replace(".", "-")
    df["exchange_rate"] = df["exchange_rate"].str.replace(",", "").astype("float")
    df["change"] = df["change"].str.replace(",", "").astype("float")

    transform_data = df.to_dict()
    return transform_data

@task
def load(schema, table, transform_data):
    conn = get_PostgreSQL_connection()
    cur = conn.cursor()
    # 원본 테이블이 없다면 생성
    create_table_sql = f"""CREATE TABLE IF NOT EXISTS {schema}.{table} (
    date date,
    exchange_rate float,
    change float,
    symbol varchar(6),
    created_date timestamp default current_timestamp
);"""
    logging.info(create_table_sql)

    # 임시 테이블 생성
    create_t_sql = f"""CREATE TEMP TABLE t AS SELECT * FROM {schema}.{table};"""
    logging.info(create_t_sql)
    try:
        cur.execute(create_table_sql)
        cur.execute(create_t_sql)
        cur.execute("COMMIT;")
    except Exception as e:
        cur.execute("ROLLBACK;")
        conn.close()
        raise

    # 임시 테이블 데이터 입력
    df = pd.DataFrame(transform_data)
    try:
        for _, row in df.iterrows():
            date, exchange_rate, change, symbol = row["date"], row["exchange_rate"], row["change"], row["symbol"]
            insert_sql = f"""INSERT INTO t (date, exchange_rate, change, symbol) 
                            VALUES ('{date}', '{exchange_rate}', '{change}', '{symbol}');"""
            logging.info(insert_sql)
    
            cur.execute(insert_sql)
        cur.execute("COMMIT;")
    except Exception as e:
        cur.execute("ROLLBACK;")
        conn.close()
        raise

    # 기존 테이블 대체
    alter_sql = f"""DELETE FROM {schema}.{table};
      INSERT INTO {schema}.{table}
      SELECT date, exchange_rate, change, symbol FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY date, symbol ORDER BY created_date DESC) seq
        FROM t
      ) AS t1
      WHERE seq = 1;"""
    logging.info(alter_sql)
    try:
        cur.execute(alter_sql)
        cur.execute("COMMIT;")
    except Exception as e:
        cur.execute("ROLLBACK;")
        conn.close()
        raise

    conn.close()

with DAG(
    dag_id = 'exchange_rate_to_raw_data',
    start_date = datetime(2022,8,24), # 날짜가 미래인 경우 실행이 안됨
    schedule = '@once',
    catchup = False
) as dag:
    start_date = '2024.02.24'
    end_date = str(datetime.now(timezone(timedelta(hours=9))))[:10].replace("-", ".")
    schema = 'raw_data'
    table = 'exchange_rate'
    transform_data = transform(extract(start_date, end_date))
    load(schema, table, transform_data)
    