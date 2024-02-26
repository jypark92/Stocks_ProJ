# stck_s3_to_rds.py

# �Լ� ����

# connect_postgresql : AWS RDS postgresql ����
# s3_open : s3�� parquet������ �����ؼ� ������������ Ÿ������ ����
# ó���϶��� ���̺��� ���� �״ϱ� ����� ó���� �ƴҶ��� ���̺��� ã�� �״´�. 

"""
#### S3���� �����͸� �о�ͼ� RDS�� �����ϴ� DAG
�� DAG�� ���� ���� 6�ÿ� ����Ǿ� S3�� �ִ� �����͸� �о�ͼ� RDS�� �����մϴ�.
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
    # PostgreSQL �����ͺ��̽��� ����
    conn = psycopg2.connect(
        host="de-1-1-database.ch4xfyi6stod.ap-northeast-2.rds.amazonaws.com",
        port="5432",
        database="dev",
        user="devde11",
        password="Devde0101"
    )
    return conn

def open_s3():

    # AWS ���� ����
    aws_access_key_id = 'AKIA4RRVVY55VLOTQLEM'
    aws_secret_access_key = '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'
    region_name = 'ap-northeast-2'
    # S3 ����
    bucket_name = 'de-1-1-bucket'
    file_key = 'stock/rawdata_past.parquet'

    # AWS S3 Ŭ���̾�Ʈ ����
    s3 = boto3.client('s3', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key, region_name=region_name)
    
    # S3���� Parquet ������ �о�� ���������������� ��ȯ
    obj = s3.get_object(Bucket=bucket_name, Key=file_key)
    df = pd.read_parquet(BytesIO(obj['Body'].read()))
    
    json_data = df.to_json()
    #print(json_data) ������������ Ÿ������ ��ȯ�� �� ����
    return json_data

def load_to_pgre(conn, df):
    try:
        # PostgreSQL ���� Ȯ��
        if conn is None:
            raise Exception("PostgreSQL conn fail")
        
        # �������������� PostgreSQL ���̺�� ����
        cur = conn.cursor()
        
        # �������������� �÷��� Ÿ���� ������� ���̺� ����
        columns = ", ".join(df.columns)
        placeholders = ", ".join(["%s" for _ in range(len(df.columns))])
        create_table_query = f"CREATE TABLE IF NOT EXISTS stck_raw ({columns})"
        cur.execute(create_table_query)
        conn.commit()
        
        # ������ ����
        values = [tuple(row) for row in df.values]
        insert_query = f"INSERT INTO stck_raw ({columns}) VALUES ({placeholders})"
        cur.executemany(insert_query, values)
        conn.commit()

        # ���� ����
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