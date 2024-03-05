import psycopg2
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

def ELT_dummydata():
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

        create_table_query = """CREATE TABLE IF NOT EXISTS analytics.stck (
                                    stck_date DATE,
                                    stocks_item VARCHAR(255),
                                    stck_clpr INT,
                                    acml_vol INT
                            );""" 
        put_data_query = """
                            INSERT INTO analytics.stck (stck_date, stocks_item, stck_clpr, acml_vol)
                            SELECT DISTINCT A.stck_date AS stck_date,
                                    B.stocks_item AS stocks_item,
                                    A.stck_clpr AS stck_clpr,
                                    A.acml_vol AS acml_vol
                            FROM raw_data.stck_raw4 A
                            JOIN raw_data.stocks_items B ON A.stck_code = CAST(B.ticker_symbol AS INT)
                        """
        
        try:
            cur.execute(create_table_query)
            conn.commit()
            cur.execute(put_data_query)
            conn.commit()
            print("Table created successfully")
            print("Yes")
        except Exception as e:
            conn.rollback()
            print(f"Failed to create table: {e}")

        cur.close()
        conn.close()
    except Exception as e:
        print(f"data load fail : {e}")

default_args = {
    'owner' : 'airflow',
    'depends_on_past' : False,
    'start_date' : datetime(2022,1,1),
    'email_on_failure' : False,
    'email_on_retry' : False,
    'retries' : 2,
    'retry_delay' : timedelta(minutes=3),
}

dag = DAG(
    'stck_dummy_analytics',
    default_args=default_args,
    schedule_interval='@once'
)

with DAG(
    dag_id='stck_dummy_analytics',
    start_date= datetime(2024,2,25),
    schedule_interval=None,
    catchup=False,
    default_args=default_args,
) as dag:
        ELT_stck_dummydata = PythonOperator(
        task_id='ELT_dummydata',
        python_callable=ELT_dummydata
    )  
        
ELT_stck_dummydata