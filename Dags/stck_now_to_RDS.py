import pandas as pd
import psycopg2
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from io import BytesIO
import boto3

aws_access_key_id = 'AKIA4RRVVY55VLOTQLEM'
aws_secret_access_key = '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'
region_name = 'ap-northeast-2'
bucket_name = 'de-1-1-bucket'


def open_s3():


    s3 = boto3.client('s3', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key, region_name=region_name)

    conn = psycopg2.connect(
        host="de-1-1-database.ch4xfyi6stod.ap-northeast-2.rds.amazonaws.com",
        port="5432",
        database="dev",
        user="devde11",
        password="Devde0101"
    )

    try:
        if conn is None:
            raise Exception("PostgreSQL conn fail")
        else: 
            print("conn success")
        cur = conn.cursor()
        
        insert_query = """INSERT INTO raw_data.stck_raw4 (stck_code, stck_date, stck_oppr, 
                                stck_clpr, stck_hipr, stck_lwpr, acml_vol, created_by) 
                                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"""

        current_date = datetime.now()
        formatted_date = current_date.strftime("%Y%m%d")

        file_key = f'stock4/date={formatted_date}/result.parquet'
        #file_key = f'stock4/date=20240304/result.parquet'

        obj = s3.get_object(Bucket=bucket_name, Key=file_key)
        df = pd.read_parquet(BytesIO(obj['Body'].read()))
        df['stck_oppr'] = df['stck_oppr'].astype(int)
        df['stck_hipr'] = df['stck_hipr'].astype(int)
        df['stck_lwpr'] = df['stck_lwpr'].astype(int)
        df['acml_vol'] = df['acml_vol'].astype(int)
        df['created_by'] = df['created_by'].astype(int)
        df = df[df['stck_date'] != 0]
        df['stck_date'] = df['stck_date'].astype(str)
        df['stck_date'] = pd.to_datetime(df['stck_date'], format='%Y-%m-%d')
        #df['stck_date'] = pd.to_datetime(df['stck_date'], format='%Y%m%d', errors='coerce').dt.date
        #df['stck_date'].fillna(pd.NaT, inplace=True)
        #df = df[df['stck_date'] != pd.NaT]
        data_list = [tuple(val for val in row) for _, row in df.iterrows()]
        cur.executemany(insert_query, data_list)
        conn.commit()
        

        cur.close()
        conn.close()

        print("data load complete")

    except Exception as e:
        print(f"data load fail : {e}")
        cur.close()
        conn.close()
        raise 

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
    'stck_now_To_RDS',
    default_args=default_args,
    schedule_interval='@daily', 
)

with DAG(
    dag_id='stck_now_To_RDS',
    start_date=datetime(2024, 2, 20),
    schedule_interval='0 18 30 * *',
    catchup=False,
    default_args=default_args,
) as dag:
        open_S3 = PythonOperator(
        task_id='open_s3',
        python_callable=open_s3
    )

open_S3 