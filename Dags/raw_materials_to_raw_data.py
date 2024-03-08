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
def extract(category_list, start_date, end_date):
    s3_hook = S3Hook(aws_conn_id='aws_conn_id')  # Airflow Connection에 정의된 AWS 연결 ID
    records = {}
    for category in category_list:
        start_key = f'{category}/year={start_date[:4]}/month={start_date[5:7]}/day={start_date[8:10]}/result.parquet'
        end_key = f'{category}/year={end_date[:4]}/month={end_date[5:7]}/day={end_date[8:10]}/result.parquet'

        df_list = []
        
        # S3 버킷 이름과 객체 키 설정
        bucket_name = 'de-1-1-bucket'
        keys = s3_hook.list_keys(bucket_name, prefix=category)
        for key in keys:
            if start_key <= key <= end_key:
                response = s3_hook.get_key(key, bucket_name).get()
                content = response['Body'].read()
                logging.info(f"{key} is loaded")
                with BytesIO(content) as f:
                    df_list.append(pd.read_parquet(f))
        records[category] = pd.concat(df_list).reset_index(drop=True).to_dict()
    return records

@task
def load(schema, records):
    conn = get_PostgreSQL_connection()
    cur = conn.cursor()
    # 원본 테이블이 없다면 생성
    for category, data in records.items():
        create_table_sql = f"""CREATE TABLE IF NOT EXISTS {schema}.{category} (
        date date,
        open float,
        high float,
        low float,
        close float,
        volume int,
        created_date timestamp default current_timestamp
    );"""
        logging.info(create_table_sql)

        # 임시 테이블 생성
        create_t_sql = f"""CREATE TEMP TABLE t_{category} AS SELECT * FROM {schema}.{category};"""
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
        df = pd.DataFrame(data)
        try:
            for _, row in df.iterrows():
                'Date', 'Open', 'High', 'Low', 'Close', 'Volume'
                date, open_pr, high, low, close, volume = row["Date"], row["Open"], row["High"], row["Low"], row["Close"], row["Volume"]
                insert_sql = f"""INSERT INTO t_{category} (date, open, high, low, close, volume) 
                                VALUES ('{date}', '{open_pr}', '{high}', '{low}', '{close}', '{volume}');"""
                logging.info(insert_sql)
        
                cur.execute(insert_sql)
            cur.execute("COMMIT;")
        except Exception as e:
            cur.execute("ROLLBACK;")
            conn.close()
            raise

        # 기존 테이블 대체
        alter_sql = f"""DELETE FROM {schema}.{category};
        INSERT INTO {schema}.{category}
        SELECT date, open, high, low, close, volume FROM (
            SELECT *, ROW_NUMBER() OVER (PARTITION BY date ORDER BY created_date DESC) seq
            FROM t_{category}
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
    dag_id = 'raw_materials_to_raw_data',
    start_date = datetime(2022,8,24), # 날짜가 미래인 경우 실행이 안됨
    schedule = '@once',
    catchup = False
) as dag:
    category_list = ['gold', 'copper', 'oil']
    now = datetime.now(timezone(timedelta(hours=9)))
    start_date = str(now)[:10]
    end_date = str(now + timedelta(days=1))[:10]
    schema = 'raw_data'
    
    records = extract(category_list, start_date, end_date)
    load(schema, records)