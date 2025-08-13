from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import os
import pandas as pd
import snowflake.connector

def test_snowflake_fetch():
    try:
        from config import DB_CREDENTIALS
        TEST_CONNECTION_NAME = 'snowflake_dev'
        credentials_for_test = DB_CREDENTIALS.get(TEST_CONNECTION_NAME)

        if not credentials_for_test:
            print(f"ERROR: Credentials for '{TEST_CONNECTION_NAME}' not found")
            return

        conn = snowflake.connector.connect(
            user=credentials_for_test.get('username'),
            password=credentials_for_test.get('password'),
            account=credentials_for_test.get('company'),
            warehouse=credentials_for_test.get('warehouse'),
            database=credentials_for_test.get('db'),
            schema=credentials_for_test.get('schema'),
            role=credentials_for_test.get('role')
        )
        df = pd.read_sql('SELECT TEST_GROUP_ID, GROUP_NAME, SCHEDULE_CRON, CREATED_AT FROM test_groups ORDER BY created_at DESC LIMIT 1000;', conn)
        print(df.head())
        conn.close()
        print("Snowflake connection closed successfully.")
    except Exception as e:
            print(f"ERROR: Failed to close Snowflake connection: {e}")
            raise

with DAG(
    dag_id="test_snowflake_fetch_dag",
    schedule_interval=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
) as dag:
    t1 = PythonOperator(
        task_id="fetch_test_groups",
        python_callable=test_snowflake_fetch,
    )
