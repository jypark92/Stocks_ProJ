from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.exceptions import AirflowException
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from datetime import datetime, timedelta, timezone
import yfinance as yf
import logging
import pandas as pd
import os

@task
def extract(symbol_dict, start_date, end_date):
    records = {}
    for category, symbol in symbol_dict.items():
        ticket = yf.Ticker(symbol)
        data = ticket.history(start=start_date, end=end_date)
        data.index = data.index.tz_convert("Asia/Seoul").strftime('%Y-%m-%d')
        df = data.reset_index()[["Date", "Open", "High", "Low", "Close", "Volume"]]
        records[category] = df.to_dict()
    return records

@task
def transform(records):
    date_dict = {}
    for category, data in records.items():

        df = pd.DataFrame(data)
        date_list = list(df['Date'].unique())
        for date in date_list:
            local_filename = Variable.get('local_data_dir') + category + "-" + date + ".parquet"
            logging.info(f"data dump at {local_filename}")
            date_df = df.loc[df['Date'] == date]
            date_df.to_parquet(local_filename, index=False)
            if os.stat(local_filename).st_size == 0:
                raise AirflowException(local_filename + " is empty")
            
        date_dict[category] = date_list

    return date_dict

@task
def load(date_dict):
    s3_bucket = Variable.get("data_s3_bucket")
    s3_hook = S3Hook(aws_conn_id = 'aws_conn_id')
    for category, date_list in date_dict.items():
        for date in date_list:
            year, month, day = date.split("-")
            filename = f"year={year}/month={month}/day={day}/result.parquet"
            s3_key = f"{category}/{filename}"

            local_filename = Variable.get('local_data_dir') + category + "-" + date + ".parquet"

            # upload it to S3
            s3_hook.load_file(
                filename=local_filename,
                key=s3_key,
                bucket_name=s3_bucket,
                replace=True
            )

            os.remove(local_filename)

with DAG(
    dag_id = 'raw_materials_to_s3',
    start_date = datetime(2024,2,21), # 날짜가 미래인 경우 실행이 안됨
    schedule = '0 8 * * MON-FRI',  # 적당히 조절
    max_active_runs = 1,
    catchup = False,
    default_args = {
        'retries': 1,
        'retry_delay': timedelta(minutes=3),

    }
) as dag:
    trigger_task = TriggerDagRunOperator(
        task_id='trigger_task',
        trigger_dag_id='raw_materials_to_raw_data',
        execution_date='{{ ds }}',
        reset_dag_run=True
    )

    symbol_dict = {
        'gold': 'GC=F', 
        'copper': 'HG=F', 
        'oil': 'CL=F'
    }
    start_date = '2021-01-01'
    end_date = '2024-02-27'

    date_dict = transform(extract(symbol_dict, start_date, end_date))
    load(date_dict) >> trigger_task