import snowflake.connector
from airflow.models import Variable
from airflow.operators.python import PythonOperator
import pandas as pd
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from datetime import datetime
from airflow import DAG
from snowflake.connector.pandas_tools import write_pandas

FILE_PATH = Variable.get('file_path')
USER = Variable.get('user')
PASSWORD = Variable.get('password')
ACCOUNT = Variable.get('account')
WAREHOUSE = Variable.get('warehouse')
DATABASE = Variable.get('database')
SCHEMA = Variable.get('schema')
TABLE = 'RAW_TABLE'
IF_EXISTS = 'replace'


def read_clean_and_load():
    raw_df = pd.read_csv(FILE_PATH)
    raw_df.drop('_id', axis=1, inplace=True)
    conn = snowflake.connector.connect(
        user=USER,
        password=PASSWORD,
        account=ACCOUNT,
        warehouse=WAREHOUSE,
        database=DATABASE,
        schema=SCHEMA
    )
    write_pandas(conn, raw_df, TABLE)


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
}

with DAG('snowflake_dag',
         start_date=datetime(2022, 8, 9),
         max_active_runs=1,
         schedule_interval='@daily',
         default_args=default_args,
         template_searchpath='/home/vlad/airflow/include',
         catchup=False
         ) as dag:
    create_raw_table = SnowflakeOperator(
        task_id='create_raw_table',
        sql='create_raw_table.sql',
        snowflake_conn_id='snowflake'
    )
    create_raw_stream = SnowflakeOperator(
        task_id='create_raw_stream',
        sql='create_raw_stream.sql',
        snowflake_conn_id='snowflake'
    )
    create_stage_table = SnowflakeOperator(
        task_id='create_stage_table',
        sql='create_stage_table.sql',
        snowflake_conn_id='snowflake'
    )
    create_stage_stream = SnowflakeOperator(
        task_id='create_stage_stream',
        sql='create_stage_stream.sql',
        snowflake_conn_id='snowflake'
    )
    create_master_table = SnowflakeOperator(
        task_id='create_master_table',
        sql='create_master_table.sql',
        snowflake_conn_id='snowflake'
    )

    insert_into_stage_table = SnowflakeOperator(
        task_id='insert_into_stage_table',
        sql='insert_into_stage_table.sql',
        snowflake_conn_id='snowflake'
    )
    insert_into_master_table = SnowflakeOperator(
        task_id='insert_into_master_table',
        sql='insert_into_master_table.sql',
        snowflake_conn_id='snowflake'
    )

    read_csv_and_load_to_snowflake = PythonOperator(
        task_id='read_csv_and_load_to_snowflake',
        python_callable=read_clean_and_load
    )

create_raw_table >> create_raw_stream >> create_stage_table >> create_stage_stream >> create_master_table >> read_csv_and_load_to_snowflake >> insert_into_stage_table >> insert_into_master_table
