# other_pastdata_to_s3.py : 금, 구리, 유가 데이터 
# 함수 구성
# extarct_data : API 호출 / 데이터 수집 / 데이터프레임 타입으로 정리 후 JSON 파일로 리턴 
# s3_export : json 파일을 받아 parquet 형태로 s3에 저장
# -*- coding: utf-8 -*-

import pandas as pd
import requests
import json
from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
import yfinance as yf
from io import StringIO, BytesIO
import boto3

def extarct_data():
    # 종목 심볼
    symbols = ['GC=F', 'HG=F', 'CL=F']

    # 데이터 다운로드
    data = yf.download(symbols, start='2021-01-01')

    # 종가 데이터만 추출하여 새로운 데이터프레임 생성
    close_prices = data['Close']

    close_prices.index = close_prices.index.strftime('%Y%m%d')

    return close_prices.to_json()

def dataTo_S3_func(json_data):
    aws_access_key_id = 'AKIA4RRVVY55VLOTQLEM'
    aws_secret_access_key = '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'
    region_name = 'ap-northeast-2'
    s3_bucket = 'de-1-1-bucket'
    s3_folder = 'others'
    s3_filename = 'rawdata_past.parquet'

    s3_path = f'{s3_folder}/{s3_filename}'

    s3_client = boto3.client('s3', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key, region_name=region_name)
    df = pd.read_json(json_data)
    with BytesIO() as f:
        df.to_parquet(f)
        f.seek(0)
        s3_client.upload_fileobj(f, s3_bucket, s3_path)

default_args = {
    'owner' : 'airflow',
    'depends_on_past' : False,
    'start_date' : datetime(2022,1,1),
    'email_on_failure' : False,
    'email_on_retry' : False,
    'retries' : 2,
    'retry_delay' : timedelta(minutes=3),
}

dag = DAG(
    'other_pastdata_to_s3',
    default_args=default_args,
    schedule_interval='@once'
)

with DAG(
    dag_id='other_pastdata_to_s3',
    start_date= datetime(2024,2,25),
    schedule_interval='0 18 * * *',
    catchup=False,
    default_args=default_args,
) as dag:
        extract = PythonOperator(
        task_id='extarct_data',
        python_callable=extarct_data
    )   
        s3_export = PythonOperator(
        task_id='dataTo_S3',
        python_callable=dataTo_S3_func,
        op_args=[extract.output]
    ) 
        
extract >> s3_export

