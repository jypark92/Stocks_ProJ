from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.exceptions import AirflowException
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

import aiohttp
import asyncio
from plugins.crawler import NewsScraper
from datetime import datetime, timedelta, timezone

import logging
import pandas as pd
import os

def get_PostgreSQL_connection():
    # autocommit is False by default
    hook = PostgresHook(postgres_conn_id='postgresql_dev_db')
    return hook.get_conn()

async def extract(schema, table, today):
    today = today.replace("-", ".")
    conn = get_PostgreSQL_connection()
    cur = conn.cursor()
    cur.execute(f"SELECT ticker_symbol FROM {schema}.{table};")
    stock_list = list(zip(*cur.fetchall()))[0]
    conn.close()

    async with aiohttp.ClientSession() as session:
        results = []
        tasks = []
        
        for stock in stock_list:
            ticker = stock
            scraper = NewsScraper(ticker, today)
            tasks.append(scraper.get_news_data(session))

        news_results = await asyncio.gather(*tasks)

        for news_result in news_results:
            results.extend(news_result)

    return results

def transform(data_list, today):
    local_filename = Variable.get('local_data_dir') + today + ".parquet"
    logging.info(f"data dump at {local_filename}")
    df = pd.DataFrame(data_list)
    df.to_parquet(local_filename, index=False)
    if os.stat(local_filename).st_size == 0:
        raise AirflowException(local_filename + " is empty")

def load(folder, today):
    s3_bucket = Variable.get("data_s3_bucket")
    year, month, day = today.split("-")
    filename = f"year={year}/month={month}/day={day}/result.parquet"
    s3_key = f"{folder}/{filename}"

    local_filename = Variable.get('local_data_dir') + today + ".parquet"

    # upload it to S3
    s3_hook = S3Hook(aws_conn_id = 'aws_conn_id')
    s3_hook.load_file(
        filename=local_filename,
        key=s3_key,
        bucket_name=s3_bucket,
        replace=True
    )

    os.remove(local_filename)

def extract_transform(schema, table, today):
    extract_result = asyncio.run(extract(schema, table, today))
    transform(extract_result, today)


with DAG(
    dag_id = 'news_to_s3',
    start_date = datetime(2024,2,21), # 날짜가 미래인 경우 실행이 안됨
    schedule = '30 10 * * MON-FRI',  # 적당히 조절
    max_active_runs = 1,
    catchup = False,
    default_args = {
        'retries': 1,
        'retry_delay': timedelta(minutes=3),

    }
) as dag:
    
    schema = "raw_data"
    table = "stocks_items"
    today = str(datetime.now(timezone(timedelta(hours=9))))[:10]
    folder = 'news'

    extract_transform_operator = PythonOperator(
        task_id='extract_transform',
        python_callable=extract_transform,
        op_kwargs={'schema': schema, 'table': table, 'today': today},
        execution_timeout=timedelta(minutes=10)
    )

    load_operator = PythonOperator(
        task_id='load',
        python_callable=load,
        op_kwargs={'folder':folder, 'today':today}
    )

    trigger_task = TriggerDagRunOperator(
        task_id='trigger_task',
        trigger_dag_id='news_to_raw_data',
        execution_date='{{ ds }}',
        reset_dag_run=True
    )
    extract_transform_operator >> load_operator >> trigger_task