# -*- coding: utf-8 -*-
import pandas as pd
import psycopg2
import json
import requests
from datetime import datetime, timedelta, timezone
import logging
from io import StringIO, BytesIO
import boto3
# -*- coding: utf-8 -*-
conn = psycopg2.connect(
        host="de-1-1-database.ch4xfyi6stod.ap-northeast-2.rds.amazonaws.com",
        port="5432",
        database="dev",
        user="devde11",
        password="Devde0101"
    )

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

try:
    # PostgreSQL ���� Ȯ��
    if conn is None:
        raise Exception("PostgreSQL conn fail")
        
    # �������������� PostgreSQL ���̺�� ����
    cur = conn.cursor()
        
    # �������������� �÷��� Ÿ���� ������� ���̺� ����
    columns = ", ".join(df.columns)
    print(columns)
    placeholders = ", ".join(["%s" for _ in range(len(df.columns))])
    print(placeholders)
except Exception as e:
    print(f"data load fail : {e}")