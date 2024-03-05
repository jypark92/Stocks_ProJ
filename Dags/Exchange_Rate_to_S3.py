from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.exceptions import AirflowException
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

import aiohttp
import asyncio
from plugins.crawler import ExchangeRateScraper
from datetime import datetime, timedelta, timezone

import logging
import pandas as pd
import os

def get_PostgreSQL_connection():
    # autocommit is False by default
    hook = PostgresHook(postgres_conn_id='postgresql_dev_db')
    return hook.get_conn()

async def extract(schema, table, end_date, start_date):
    end_date = end_date.replace("-", ".")
    conn = get_PostgreSQL_connection()
    cur = conn.cursor()
    cur.execute(f"SELECT symbol FROM {schema}.{table};")
    currency_list = list(zip(*cur.fetchall()))[0]
    conn.close()

    async with aiohttp.ClientSession() as session:
        results = []
        tasks = []
        
        for currency in currency_list:
            symbol = currency
            scraper = ExchangeRateScraper(symbol, end_date, start_date)
            tasks.append(scraper.get_news_data(session))

        exchange_rate_results = await asyncio.gather(*tasks)

        for exchange_rate_result in exchange_rate_results:
            results.extend(exchange_rate_result)

    return results

def transform(data_list):
    df = pd.DataFrame(data_list)
    date_list = list(df['date'].unique())
    for date in date_list:
        local_filename = Variable.get('local_data_dir') + date.replace(".", "-") + ".parquet"
        logging.info(f"data dump at {local_filename}")
        date_df = df.loc[df['date'] == date]
        date_df.to_parquet(local_filename, index=False)
        if os.stat(local_filename).st_size == 0:
            raise AirflowException(local_filename + " is empty")
        
    return date_list

def load(folder, date_list):
    s3_bucket = Variable.get("data_s3_bucket")
    # upload it to S3
    s3_hook = S3Hook(aws_conn_id = 'aws_conn_id')
    for date in date_list:
        year, month, day = date.split(".")
        filename = f"year={year}/month={month}/day={day}/result.parquet"
        s3_key = f"{folder}/{filename}"

        local_filename = Variable.get('local_data_dir') + date.replace(".", "-") + ".parquet"

        s3_hook.load_file(
            filename=local_filename,
            key=s3_key,
            bucket_name=s3_bucket,
            replace=True
        )

        os.remove(local_filename)

def etl(schema, table, end_date, start_date, folder):
    extract_result = asyncio.run(extract(schema, table, end_date, start_date))
    load(folder, transform(extract_result))

with DAG(
    dag_id = 'exchange_rate_to_s3',
    start_date = datetime(2024,2,21), # 날짜가 미래인 경우 실행이 안됨
    schedule = '0 7 * * MON-FRI',  # 적당히 조절
    max_active_runs = 1,
    catchup = False,
    default_args = {
        'retries': 1,
        'retry_delay': timedelta(minutes=3),

    }
) as dag:
    
    schema = "raw_data"
    table = "currency"
    today = str(datetime.now(timezone(timedelta(hours=9))))[:10]
    folder = 'exchange_rate'
    etl_operator = PythonOperator(
        task_id='etl',
        python_callable=etl,
        op_kwargs={'schema': schema, 'table': table, 'end_date': today, 'start_date': '2024.02.24', 'folder': folder},
        execution_timeout=timedelta(minutes=10)
    )

    trigger_task = TriggerDagRunOperator(
        task_id='trigger_task',
        trigger_dag_id='exchange_rate_to_raw_data',
        execution_date='{{ ds }}',
        reset_dag_run=True
    )
    
    etl_operator >> trigger_task