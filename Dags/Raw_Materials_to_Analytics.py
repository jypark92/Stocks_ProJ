from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook

from datetime import datetime
import logging

def get_PostgreSQL_connection():
    # autocommit is False by default
    hook = PostgresHook(postgres_conn_id='postgresql_dev_db')
    return hook.get_conn()

@task
def elt(raw_schema, schema, stock_items_tbl, stocks_tbl, exchange_tbl, oil_tbl, gold_tbl, copper_tbl, table):
    conn=get_PostgreSQL_connection()

    cur = conn.cursor()
    # 원본 테이블이 없다면 생성
    create_table_sql = f"""CREATE TABLE IF NOT EXISTS {schema}.{table} (
                                date date,
                                stocks_item varchar(32),
                                stocks_pr int,
                                dollar_pr float,
                                oil_pr float,
                                gold_pr float,
                                copper_pr float
                           );"""

    logging.info(create_table_sql)
    # 임시 테이블 생성
    create_t_sql = f"""CREATE TEMP TABLE t AS SELECT * FROM {schema}.{table};"""
    logging.info(create_t_sql)
    try:
        cur.execute(create_table_sql)
        cur.execute(create_t_sql)
        cur.execute("COMMIT;")
    except Exception as e:
        cur.execute("ROLLBACK;")
        conn.close()
        raise

    # 기존 테이블 대체
    try:
        cur.execute(f"DELETE FROM {schema}.{table};")
        alter_sql = f"""INSERT INTO {schema}.{table}
                        SELECT date, stocks_item, stocks_pr, dollar_pr, oil_pr, gold_pr, copper_pr
                        FROM (
                            SELECT er.date, 
                                stocks_item, 
                                sr.stck_clpr AS stocks_pr, 
                                er.exchange_rate AS dollar_pr, 
                                TRUNC(o.close * exchange_rate * 100 + 0.5) / 100 AS oil_pr, 
                                TRUNC(g.close * exchange_rate * 100 + 0.5) / 100 AS gold_pr,
                                TRUNC(c.close * exchange_rate * 100 + 0.5) / 100 AS copper_pr, 
                            ROW_NUMBER() 
                            OVER (PARTITION BY er.date, stocks_item 
                            ORDER BY sr.created_by, er.created_date, o.created_date, g.created_date, c.created_date DESC) seq
                            FROM {raw_schema}.{stocks_tbl} AS sr
                            JOIN {raw_schema}.{stock_items_tbl} AS si
                            ON sr.stck_code = CAST(si.ticker_symbol AS INT)
                            JOIN (
                            SELECT date, exchange_rate, created_date
                            FROM {raw_schema}.{exchange_tbl}
                            WHERE symbol = 'USDKRW'
                            ) AS er
                            ON sr.stck_date = er.date
                            JOIN {raw_schema}.{oil_tbl} AS o
                            ON er.date = o.date
                            JOIN {raw_schema}.{gold_tbl} AS g
                            ON o.date = g.date
                            JOIN {raw_schema}.{copper_tbl} AS c
                            ON g.date = c.date
                            ) AS t1
                        WHERE seq = 1;"""
        logging.info(alter_sql)
    
        cur.execute(alter_sql)
        cur.execute("COMMIT;")
    except Exception as e:
        cur.execute("ROLLBACK;")
        conn.close()
        raise

    conn.close()

with DAG(
    dag_id = 'raw_materials_to_analytics',
    start_date = datetime(2023,5,30), # 날짜가 미래인 경우 실행이 안됨
    schedule = '30 9 * * MON-FRI',
    catchup = False
) as dag:
    raw_schema = 'raw_data'
    schema = 'analytics'
    stock_items_tbl = 'stocks_items'
    stocks_tbl = 'stck_raw4'
    exchange_tbl = 'exchange_rate'
    oil_tbl = 'oil'
    gold_tbl = 'gold'
    copper_tbl = 'copper'
    table = 'stocks_others'

    elt(raw_schema, schema, stock_items_tbl, stocks_tbl, exchange_tbl, oil_tbl, gold_tbl, copper_tbl, table)