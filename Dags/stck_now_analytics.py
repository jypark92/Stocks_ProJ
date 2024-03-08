import psycopg2
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from airflow.models import Variable

def ELT_nowdata():
    conn = psycopg2.connect(
        host=Variable.get("rds_host"),
        port=Variable.get("rds_port"),
        database="dev",
        user=Variable.get("rds_user"),
        password=Variable.get("rds_pw"),
    )

    try:
        if conn is None:
            raise Exception("PostgreSQL conn fail")
        else: 
            print("conn success")
        cur = conn.cursor()

        formatted_time = datetime.now().strftime("%Y%m%d")

        put_data_query = f"""
                            INSERT INTO analytics.stck (stck_date, stocks_item, stck_clpr, acml_vol)
                            SELECT DISTINCT A.stck_date AS stck_date,
                                    B.stocks_item AS stocks_item,
                                    A.stck_clpr AS stck_clpr,
                                    A.acml_vol AS acml_vol
                            FROM raw_data.stck_raw4 A
                            JOIN raw_data.stocks_items B ON A.stck_code = CAST(B.ticker_symbol AS INT)
                            WHERE A.stck_date = %s;
                        """
        
        try:

            cur.execute(put_data_query, (formatted_time,))
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
    'email_on_failure' : False,
    'email_on_retry' : False,
    'retries' : 2,
    'retry_delay' : timedelta(minutes=3),
}



with DAG(
    dag_id='stck_now_ELT',
    start_date = datetime(2024,3,4), 
    schedule = '* 10 * * MON-FRI',  
    catchup=False,
    default_args=default_args,
) as dag:
        ELT_stck_nowdata = PythonOperator(
        task_id='ELT_nowdata',
        python_callable=ELT_nowdata
    )  
        
ELT_stck_nowdata