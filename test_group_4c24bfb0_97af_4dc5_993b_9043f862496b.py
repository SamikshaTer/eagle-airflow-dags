
import sys
sys.path.append("/home/admsamikshat")
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from shared_config.config import DB_CREDENTIALS

def run_adhoc_test_logic(test_case_id):
    creds = DB_CREDENTIALS.get('snowflake_dev')
    if creds:
        print(f"Using Snowflake credentials for {test_case_id}")
        # Example: connect to Snowflake using creds dictionary here
    else:
        print("Credentials for snowflake_dev not found.")
    print(f"Executing test case: {test_case_id}")

with DAG(
    dag_id="test_group_4c24bfb0_97af_4dc5_993b_9043f862496b",
    schedule_interval="55 6 * * *",
    start_date=datetime(2023, 1, 1),
    catchup=False,
) as dag:
    
    t1 = PythonOperator(
        task_id="case_1",
        python_callable=run_adhoc_test_logic,
        op_args=["3e598c0c-3a99-4127-ae1a-11c48750f289"],
    )
    
    t2 = PythonOperator(
        task_id="case_2",
        python_callable=run_adhoc_test_logic,
        op_args=["f225606b-21c0-4e82-b978-12c4cb77f161"],
    )
    
    
    t1 >> t2
    