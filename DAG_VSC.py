import boto3
import pandas as pd
import logging
import json
from io import StringIO
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta, timezone
from airflow.models import Variable
import requests
import s3fs
from airflow.decorators import task
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import psycopg2

app_key = "PS4WfYz9jA72Rkr00VUWH186hG5t5ub3DKgQ"
app_secret = "SXm89J7z+net8nllTYL6EL6Dy1DZX/PAOAYLT1eLRc7hzYylB7iW0RGpKvSU9necFG1xcbtdFZrqcAI7/poMz3siCjsz0XpirAnl9W66EVo3jDhUMQhz06BzRTQlNZiLjb1zi0MNdeuzbiNkUPtaxbfX8ypT/S9nOSthwkbWCGyTLLp264Y="

# 접근토큰발급 (하루에 한번)
@task
def make_token():
    url_base = "https://openapi.koreainvestment.com:9443"
    path = "/oauth2/tokenP"
    data = {
    "grant_type": "client_credentials",
    "appkey": app_key,
    "appsecret": app_secret
    }
    response = requests.post(url_base + path, json=data, headers={'Content-Type': 'application/json'})
    a = response.json()
    return a['access_token']

# 20210101~ 100개 주식 수집
@task
def past_stck(access_token2):
    url_base = "https://openapi.koreainvestment.com:9443"
    path = "/uapi/domestic-stock/v1/quotations/inquire-daily-itemchartprice"
    tr_id = "FHKST03010100"
    access_token = access_token2

    code_list = [
    "005930", "000660", "373220", "207940", "005935", "005380", "000270", "068270", "005490", "051910",
    "035420", "028260", "006400", "035720", "105560", "003670", "055550", "012330", "066570", "032830",
    "086790", "003550", "138040", "323410", "000810", "450080", "034730", "015760", "096770", "018260",
    "011200", "033780", "259960", "017670", "316140", "024110", "047050", "009150", "030200", "329180",
    "034020", "010130", "022100", "352820", "402340", "003490", "009540", "010950", "042700", "012450",
    "090430", "357870", "069500", "459580", "042660", "005830", "161390", "377300", "326030", "086280",
    "010140", "005070", "011170", "009830", "066970", "001570", "267250", "251270", "088980", "005387",
    "454910", "006800", "449170", "051900", "047810", "361610", "000100", "302440", "004020", "241560",
    "028050", "180640", "011070", "423160", "036570", "097950", "078930", "032640", "011780", "307950",
    "267260", "128940", "034220", "021240", "029780", "271560", "005940", "071050", "000720", "035250"
    ]

    code_list2 = ["005930", "000660", "373220"]
    
    current_date = datetime.now()
    formatted_date = current_date.strftime("%Y%m%d")

    data = {
    'stck_code': [],
    'stck_date': [],
    'stck_oppr': [],
    'stck_clpr': [],
    'stck_hipr': [],
    'stck_lwpr': [],
    'stck_vol': [],
    'created_by' : []
    }

    def mak_data(code,day):
        data2 = {
            "fid_cond_mrkt_div_code": "J",
            "fid_input_date_1": "20210101",
            "fid_input_date_2": day,
            "fid_input_iscd": code,
            "fid_org_adj_prc": "0",
            "fid_period_div_code": "D"
        }
        return data2

    header = {
        "Content-Type": "application/json", 
        "authorization": f"Bearer {access_token}",
        "appkey": app_key,
        "appsecret": app_secret,
        "tr_id": tr_id
    }

    div_peroid = ["20240217", "20230917", "20230423", "20221129", "20220705", "20220208", "20210908", "20210418"]

    for code in code_list2:
        for day in div_peroid:
            response = requests.get(url_base + path, params=mak_data(code,day), headers=header)
            for i in response.json()['output2']:
                data['stck_code'].append(code)
                data['stck_date'].append(i['stck_bsop_date'])
                data['stck_oppr'].append(i['stck_oprc'])
                data['stck_clpr'].append(i['stck_clpr'])
                data['stck_hipr'].append(i['stck_hgpr'])
                data['stck_lwpr'].append(i['stck_lwpr'])
                data['stck_vol'].append(i['acml_vol'])
                data['created_by'].append(formatted_date)
                
    df = pd.DataFrame(data)

    return df

# parquet 파일로 S3로 적재

@task
def dataTo_S3(df):
    aws_access_key_id = 'AKIA4RRVVY55VLOTQLEM'
    aws_secret_access_key = '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'
    region_name = 'ap-northeast-2'
    s3_bucket = 'de-1-1-bucket'
    s3_folder = 'stock'
    s3_filename = 'rawdata_past.parquet'

    s3_path = f's3://{s3_bucket}/{s3_folder}/{s3_filename}'

    s3 = s3fs.S3FileSystem(key=aws_access_key_id, secret=aws_secret_access_key, endpoint_url='https://s3.ap-northeast-2.amazonaws.com')

    with s3.open(s3_path, 'wb') as f:
        df.to_parquet(f)

@task
def dataTo_RDS(schema, table):
    # RDS 연결 정보
    host = 'de-1-1-database.ch4xfyi6stod.ap-northeast-2.rds.amazonaws.com'
    username = 'devde11'
    password = 'Devde0101'
    port = 5432
    dbname = 'dev'
    
    # S3에서 데이터 읽어오기
    aws_access_key_id = 'AKIA4RRVVY55VLOTQLEM'
    aws_secret_access_key = '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'
    s3_bucket = 'de-1-1-bucket'
    s3_folder = 'stock'
    s3_filename = 'rawdata_past.parquet'
    s3_path = f's3://{s3_bucket}/{s3_folder}/{s3_filename}'

    df = pd.read_parquet(s3_path)
    
    # RDS에 데이터 적재
    conn = psycopg2.connect(host=host, dbname=dbname, user=username, password=password, port=port)
    cur = conn.cursor()

    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS {schema}.{table} (
        stck_code	            number,
        stck_date           	datetime,
        stck_oppr	            number,
        stck_hipr               number,
        stck_lwpr       	    number,
        stck_clpr       	    number,
        stck_vol                datetime,
        created_date	        datetime,
        PRIMARY KEY(stck_code, stck_date)
    );
    """
    
    create_t_sql = f"""CREATE TEMP TABLE t AS SELECT * FROM {schema}.{table};"""

    for _, row in df.iterrows():
        query = f"""INSERT INTO {schema}.{table} 
        (stck_code, stck_date, stck_oppr, stck_hipr, stck_clpr, stck_vol, created_date) 
        VALUES
        (   {int(row['stck_code'])}, 
            {datetime.datetime.strptime(row['stck_date'], '%Y%m%d').date()}, 
            {int(row['stck_oppr'])}, 
            {int(row['stck_hipr'])}, 
            {int(row['stck_lwpr'])}, 
            {int(row['stck_clpr'])}, 
            {int(row['stck_vol'])}, 
            {datetime.datetime.strptime(row['created_date'], '%Y%m%d').date()}
        );
        """
        cur.execute(query)
    
    conn.commit()
    cur.close()
    conn.close()

with DAG(
    dag_id = 'PastData_S3_RDS',
    start_date=datetime(2024, 2, 22),               # DAG 실행 시작 날짜
    schedule_interval=None,                         # DAG의 실행 간격 (한 번만 실행됨)
    catchup=False,                                  # 이전에 실행되었던 작업을 재실행하지 않음
    default_args={
        'retries': 2,                               # 작업 재시도 횟수
        'retry_delay': timedelta(minutes=3),        # 작업 재시도 간격
    }
) as dag:
    access_token = make_token()
    result_df = past_stck(access_token)
    dataTo_S3(result_df) >> dataTo_RDS("raw_data", "stck")

