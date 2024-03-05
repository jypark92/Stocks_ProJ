from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from datetime import datetime

import logging
import pandas as pd
from io import BytesIO

def get_PostgreSQL_connection():
    # autocommit is False by default
    hook = PostgresHook(postgres_conn_id='postgresql_dev_db')
    return hook.get_conn()
@task
def extract():
    s3_hook = S3Hook(aws_conn_id='aws_conn_id')  # Airflow Connection에 정의된 AWS 연결 ID
    # S3 버킷 이름과 객체 키 설정
    bucket_name = 'de-1-1-bucket'
    keys = s3_hook.list_keys(bucket_name, prefix='news')
    key = max(keys)

    response = s3_hook.get_key(key, bucket_name).get()
    content = response['Body'].read()
    with BytesIO(content) as f:
        records = pd.read_parquet(f).reset_index(drop=True).to_dict()

    return records

@task
def transform(records):
    df = pd.DataFrame(records)    
    df["title"] = df["title"].str.replace("'","").replace('"','')
    df["published_time"] = df["published_time"].str.replace(".", "-")
    transform_data = df.to_dict()

    return transform_data

@task
def load(schema, table, transform_data):
    conn = get_PostgreSQL_connection()
    cur = conn.cursor()
    drop_recreate_sql = f"""DROP TABLE IF EXISTS {schema}.{table};
CREATE TABLE {schema}.{table} (
    published_time timestamp,
    ticker_symbol varchar(6),
    title varchar(128),
    created_date timestamp default current_timestamp
);
"""
    try:
        cur.execute(drop_recreate_sql)
        cur.execute("Commit;")
        logging.info(drop_recreate_sql)
    except Exception as e:
        cur.execute("Rollback;")
        logging.info(e)
        conn.close()
        raise 

    df = pd.DataFrame(transform_data)
    try:
        for _, row in df.iterrows():
            title, published_time, ticker_symbol = row["title"], row["published_time"], row["ticker_symbol"]
            insert_sql = f"""INSERT INTO {schema}.{table} (published_time, ticker_symbol, title) 
                            VALUES ('{published_time}', '{ticker_symbol}', '{title}');"""
            logging.info(insert_sql)
    
            cur.execute(insert_sql)
        cur.execute("COMMIT;")
    except Exception as e:
        cur.execute("ROLLBACK;")
        conn.close()
        raise

    conn.close()

with DAG(
    dag_id = 'news_to_raw_data',
    start_date = datetime(2023,5,30), # 날짜가 미래인 경우 실행이 안됨
    schedule = '@once',
    catchup = False
) as dag:
    trigger_task = TriggerDagRunOperator(
        task_id='trigger_task',
        trigger_dag_id='news_to_analytics',
        execution_date='{{ ds }}',
        reset_dag_run=True
    )

    schema = 'raw_data'
    table = 'news'
    transform_data = transform(extract())
    load(schema, table, transform_data) >> trigger_task