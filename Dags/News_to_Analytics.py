from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook

from datetime import datetime

import logging
import pandas as pd
from konlpy.tag import Hannanum
import re
def get_PostgreSQL_connection():
    # autocommit is False by default
    hook = PostgresHook(postgres_conn_id='postgresql_dev_db')
    return hook.get_conn()

def normalize(text):
    replaced_text = re.sub("[^\w가-힣]+"," ", re.sub("\[[^\[\]]*\]", "", text)).strip()
    return replaced_text

@task
def elt(raw_schema, schema, raw_table1, raw_table2, table1, table2):
    conn=get_PostgreSQL_connection()

    cur = conn.cursor()
    drop_recreate_sql = f"""DROP TABLE IF EXISTS {schema}.{table1};
    DROP TABLE IF EXISTS {schema}.{table2};
    CREATE TABLE {schema}.{table1} (
        published_time timestamp,
        stocks_item varchar(32),
        title varchar(128)
    );
    CREATE TABLE {schema}.{table2} (
        published_time timestamp,
        stocks_item varchar(32),
        word varchar(32)
    );
    """
    logging.info(drop_recreate_sql)
    try:
        cur.execute(drop_recreate_sql)
        cur.execute("Commit;")
    except Exception as e:
        cur.execute("Rollback;")
        conn.close()
        raise

    # 기존 테이블 대체
    try:
        insert_sql = f"""INSERT INTO {schema}.{table1}
                        SELECT published_time, stocks_item, title 
                        FROM {raw_schema}.{raw_table1} AS n
                        JOIN  {raw_schema}.{raw_table2} AS s
                        ON n.ticker_symbol = s.ticker_symbol;"""
        logging.info(insert_sql)
    
        cur.execute(insert_sql)
        cur.execute("COMMIT;")
    except Exception as e:
        cur.execute("ROLLBACK;")
        conn.close()
        raise

    sql = f"SELECT * FROM {schema}.{table1};"
    df = pd.read_sql(sql, con=conn)

    tagger = Hannanum()
    df["word"] = df["title"].apply(normalize).apply(tagger.nouns)
    exploded_df = df.explode(column="word")[["published_time", "stocks_item", "word"]]
    try:
        for _, row in exploded_df.iterrows():
            published_time, stocks_item, word = row["published_time"], row["stocks_item"], row["word"] 
            insert_sql = f"""INSERT INTO {schema}.{table2} (published_time, stocks_item, word) 
                            VALUES ('{published_time}', '{stocks_item}', '{word}');"""
            logging.info(insert_sql)
    
            cur.execute(insert_sql)
        cur.execute("COMMIT;")
    except Exception as e:
        cur.execute("ROLLBACK;")
        conn.close()
        raise

    conn.close()

with DAG(
    dag_id = 'news_to_analytics',
    start_date = datetime(2023,5,30), # 날짜가 미래인 경우 실행이 안됨
    schedule = '@once',
    catchup = False
) as dag:
    raw_schema = 'raw_data'
    schema = 'analytics'
    raw_table1 = 'news'
    raw_table2 = 'stocks_items'
    table1 = 'news'
    table2 = 'news_words'
    elt(raw_schema, schema, raw_table1, raw_table2, table1, table2)
