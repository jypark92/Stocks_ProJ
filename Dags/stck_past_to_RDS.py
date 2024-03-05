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

        create_table_query = """CREATE TABLE IF NOT EXISTS raw_data.stck_raw4(
                                stck_code INT, 
                                stck_date DATE, 
                                stck_oppr INT, 
                                stck_clpr INT, 
                                stck_hipr INT, 
                                stck_lwpr INT, 
                                acml_vol INT, 
                                created_by INT)
                                """
        
        try:
            cur.execute(create_table_query)
            conn.commit()
            print("Table created successfully")
            print("Yes")
        except Exception as e:
            conn.rollback()
            print(f"Failed to create table: {e}")

        insert_query = """INSERT INTO raw_data.stck_raw4 (stck_code, stck_date, stck_oppr, 
                                stck_clpr, stck_hipr, stck_lwpr, acml_vol, created_by) 
                                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"""

        paginator = s3.get_paginator('list_objects')

        for result in paginator.paginate(Bucket=bucket_name, Prefix='stock4'):

            for obj in result.get('Contents', []):
                file_key = obj['Key']
                if file_key.endswith('result.parquet'):
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
    'stck_past_To_RDS',
    default_args=default_args,
    schedule_interval='@once', 
)

with DAG(
    dag_id='stck_past_To_RDS',
    start_date=datetime(2024, 2, 20),
    schedule_interval=None,
    catchup=False,
    default_args=default_args,
) as dag:
        open_S3 = PythonOperator(
        task_id='open_s3',
        python_callable=open_s3
    )

open_S3 