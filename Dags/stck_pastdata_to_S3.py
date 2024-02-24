# stck_pastdata_to_s3 코드

import json
import requests
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta, timezone
import logging
import pandas as pd
from io import StringIO, BytesIO
import boto3

app_key = "PS4WfYz9jA72Rkr00VUWH186hG5t5ub3DKgQ"
app_secret = "SXm89J7z+net8nllTYL6EL6Dy1DZX/PAOAYLT1eLRc7hzYylB7iW0RGpKvSU9necFG1xcbtdFZrqcAI7/poMz3siCjsz0XpirAnl9W66EVo3jDhUMQhz06BzRTQlNZiLjb1zi0MNdeuzbiNkUPtaxbfX8ypT/S9nOSthwkbWCGyTLLp264Y="

def Make_token():
    url_base = "https://openapi.koreainvestment.com:9443"
    path = "/oauth2/tokenP"
    data = {
        "grant_type": "client_credentials",
        "appkey": app_key,
        "appsecret": app_secret
    }
    response = requests.post(url_base + path, json=data, headers={'Content-Type': 'application/json'})
    result = response.json()
    return result['access_token']

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
    'acml_vol': [],
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
    div_peroid2 = ["20240217", "20230917", "20230423", "20221129","20220705","20220208"]
    for code in code_list:
        for day in div_peroid:
            response = requests.get(url_base + path, params=mak_data(code, day), headers=header)
            a = response.json()
            print(a)
            for i in a.get('output2', []):
                data['stck_code'].append(code)
                data['stck_date'].append(i.get('stck_bsop_date', "0"))
                data['stck_oppr'].append(i.get('stck_oprc', "0"))
                data['stck_clpr'].append(i.get('stck_clpr', "0"))
                data['stck_hipr'].append(i.get('stck_hgpr', "0"))
                data['stck_lwpr'].append(i.get('stck_lwpr', "0"))
                data['acml_vol'].append(i.get('acml_vol', "0"))
                data['created_by'].append(formatted_date)
                
    df = pd.DataFrame(data)
    json_data = df.to_json()
    return json_data

def dataTo_S3_func(json_data):
    aws_access_key_id = 'AKIA4RRVVY55VLOTQLEM'
    aws_secret_access_key = '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'
    region_name = 'ap-northeast-2'
    s3_bucket = 'de-1-1-bucket'
    s3_folder = 'stock'
    s3_filename = 'rawdata_past.parquet'

    s3_path = f'{s3_folder}/{s3_filename}'

    s3_client = boto3.client('s3', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key, region_name=region_name)
    df = pd.read_json(json_data)
    with BytesIO() as f:
        df.to_parquet(f)
        f.seek(0)
        s3_client.upload_fileobj(f, s3_bucket, s3_path)

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
    'A_PastData_S3_RDS',
    default_args=default_args,
    schedule_interval='@once', 
)

with DAG(
    dag_id='B_PastData_S3_RDS',
    start_date=datetime(2024, 2, 20),
    schedule_interval=None,
    catchup=False,
    default_args=default_args,
) as dag:
        access_token3 = PythonOperator(
        task_id='make_token',
        python_callable=Make_token
    )
        result_df = PythonOperator(
        task_id='past_stck',
        python_callable=past_stck,
        op_args=[access_token3.output]
    )
        s3_export = PythonOperator(
        task_id='dataTo_S3',
        python_callable=dataTo_S3_func,
        op_args=[result_df.output]
    )

# 함수 구성 
# access_token3 : 접근 토큰 발급 
# result_df : API 호출 / 데이터 수집 / 데이터프레임 타입으로 정리 후 JSON 파일로 리턴 
# s3_export : json 파일을 받아 parquet 형태로 s3에 저장

access_token3 >> result_df >> s3_export