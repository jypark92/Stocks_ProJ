# stck_nowdata_to_s3 code

# 함수 구성
# access_token3 : 접근 토큰 발급 
# result_df : API 호출 / 데이터 수집 / 데이터프레임 타입으로 정리 후 JSON 파일로 리턴
# s3_export : 기존에 s3에 있는 parquet 파일을 열고 New data(json 파일)을 받아 추가하고 다시 parquet 형태로 s3에 저장

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

def now_stck(access_token2):
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

    header = {
        "Content-Type": "application/json", 
        "authorization": f"Bearer {access_token}",
        "appkey": app_key,
        "appsecret": app_secret,
        "tr_id": tr_id
    }

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

    current_date = datetime.now()
    formatted_date = current_date.strftime("%Y%m%d")

    def mak_data(code,day):
        data2 = {
            "fid_cond_mrkt_div_code": "J",
            "fid_input_date_1": day,
            "fid_input_date_2": day,
            "fid_input_iscd": code,
            "fid_org_adj_prc": "0",
            "fid_period_div_code": "D"
        }
        return data2

    for code in code_list:
        response = requests.get(url_base + path, params=mak_data(code, formatted_date), headers=header)
        a = response.json()['output2'][0]
        print(a)
        
        data['stck_code'].append(code)
        data['stck_oppr'].append(a.get('stck_oprc', "0"))
        data['stck_clpr'].append(a.get('stck_clpr', "0"))
        data['stck_hipr'].append(a.get('stck_hgpr', "0"))
        data['stck_lwpr'].append(a.get('stck_lwpr', "0"))
        data['acml_vol'].append(a.get('acml_vol', "0"))
        data['created_by'].append(formatted_date)
    
    df = pd.DataFrame(data)
    json_data = df.to_json()
    return json_data

def update_s3file(json_data):
    # AWS 계정 액세스 키 및 지역 설정
    aws_access_key_id = 'AKIA4RRVVY55VLOTQLEM'
    aws_secret_access_key = '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'
    region_name = 'ap-northeast-2'
    
    # S3 버킷 및 파일 경로 설정
    s3_bucket = 'de-1-1-bucket'
    s3_folder = 'stock'
    s3_filename = 'rawdata_past.parquet'

    s3_path = f'{s3_folder}/{s3_filename}'

    # AWS S3 클라이언트 생성
    s3_client = boto3.client('s3', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key, region_name=region_name)
    
    # 기존 Parquet 파일을 S3에서 다운로드
    with BytesIO() as data:
        s3_client.download_fileobj(s3_bucket, s3_path, data)
        data.seek(0)
        df_existing = pd.read_parquet(data)
    
    # 새로운 JSON 데이터를 읽기
    df_new = pd.read_json(json_data)
    
    # 새 데이터를 기존 DataFrame에 추가
    df_updated = pd.concat([df_existing, df_new], ignore_index=True)
    
    # 새로운 데이터를 Parquet 형식으로 변환
    with BytesIO() as f:
        df_updated.to_parquet(f)
        f.seek(0)
        
        # 기존 파일 삭제
        s3_client.delete_object(Bucket=s3_bucket, Key=s3_path)
        
        # 업데이트된 Parquet 파일을 S3에 업로드
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
    'A_NowData_S3_RDS',
    default_args=default_args,
    schedule_interval='@once', 
)

with DAG(
    dag_id='A_NowData_S3_RDS',
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
        task_id='now_stck',
        python_callable=now_stck,
        op_args=[access_token3.output]
    )
        s3_export = PythonOperator(
        task_id='dataTo_S3',
        python_callable=update_s3file,
        op_args=[result_df.output]
    )

access_token3 >> result_df >> s3_export