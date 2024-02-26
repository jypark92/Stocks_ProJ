# stck_s3_to_rds.py

# 함수 구성

# connect_postgresql : AWS RDS postgresql 연결
# s3_open : s3에 parquet파일을 오픈해서 데이터프레임 타입으로 변경
# 처음일때는 테이블이 없을 테니까 만들고 처음이 아닐때는 테이블을 찾고 쌓는다. 

"""
#### S3에서 데이터를 읽어와서 RDS로 적재하는 DAG
이 DAG는 매일 오후 6시에 실행되어 S3에 있는 데이터를 읽어와서 RDS로 적재합니다.
"""

import pandas as pd
import psycopg2
import json
import requests
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta, timezone
import logging
from io import StringIO, BytesIO
import boto3

def conn_postgre():
    # PostgreSQL 데이터베이스에 연결
    conn = psycopg2.connect(
        host="de-1-1-database.ch4xfyi6stod.ap-northeast-2.rds.amazonaws.com",
        port="5432",
        database="dev",
        user="devde11",
        password="Devde0101"
    )
    return conn

def open_s3():

    # AWS 계정 정보
    aws_access_key_id = 'AKIA4RRVVY55VLOTQLEM'
    aws_secret_access_key = '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'
    region_name = 'ap-northeast-2'
    # S3 정보
    bucket_name = 'de-1-1-bucket'
    file_key = 'stock/rawdata_past.parquet'

    # AWS S3 클라이언트 생성
    s3 = boto3.client('s3', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key, region_name=region_name)
    
    # S3에서 Parquet 파일을 읽어와 데이터프레임으로 반환
    obj = s3.get_object(Bucket=bucket_name, Key=file_key)
    df = pd.read_parquet(BytesIO(obj['Body'].read()))
    
    json_data = df.to_json()
    #print(json_data) 데이터프레임 타입으로 반환할 수 없음
    return json_data

def load_to_pgre(conn, df):
    try:
        # PostgreSQL 연결 확인
        if conn is None:
            raise Exception("PostgreSQL conn fail")
        
        # 데이터프레임을 PostgreSQL 테이블로 적재
        cur = conn.cursor()
        
        # 데이터프레임의 컬럼과 타입을 기반으로 테이블 생성
        columns = ", ".join(df.columns)
        placeholders = ", ".join(["%s" for _ in range(len(df.columns))])
        create_table_query = f"CREATE TABLE IF NOT EXISTS stck_raw ({columns})"
        cur.execute(create_table_query)
        conn.commit()
        
        # 데이터 적재
        values = [tuple(row) for row in df.values]
        insert_query = f"INSERT INTO stck_raw ({columns}) VALUES ({placeholders})"
        cur.executemany(insert_query, values)
        conn.commit()

        # 연결 종료
        cur.close()
        conn.close()
        
        print("data load complete")
    except Exception as e:
        print(f"data load fail : {e}")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2022, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=3),
}

dag = DAG(
    'A_S3_to_RDS',
    default_args=default_args,
    schedule_interval='@once', 
)

with DAG(
    dag_id='A_S3_to_RDS',
    start_date=datetime(2024, 2, 20),
    schedule_interval='0 18 * * *',
    catchup=False,
    default_args=default_args,
) as dag:
        conn_RDS = PythonOperator(
        task_id='conn_postgre',
        python_callable=conn_postgre
    )
        open_S3 = PythonOperator(
        task_id='open_s3',
        python_callable=open_s3
    )
        load_to_RDS = PythonOperator(
        task_id='load_to_pgre',
        python_callable=load_to_pgre,
        op_args=[conn_RDS.output,open_S3.output]
    )
        
conn_RDS >> open_S3 >> load_to_RDS