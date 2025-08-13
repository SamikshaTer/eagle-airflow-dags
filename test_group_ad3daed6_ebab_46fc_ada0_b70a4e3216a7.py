
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
    dag_id="test_group_ad3daed6_ebab_46fc_ada0_b70a4e3216a7",
    schedule_interval="16 6 * * *",
    start_date=datetime(2023, 1, 1),
    catchup=False,
) as dag:
    
    t1 = PythonOperator(
        task_id="case_1",
        python_callable=run_adhoc_test_logic,
        op_args=["f225606b-21c0-4e82-b978-12c4cb77f161"],
    )
    
    t2 = PythonOperator(
        task_id="case_2",
        python_callable=run_adhoc_test_logic,
        op_args=["41501c34-e46f-47f9-9fb8-42825514f6ad"],
    )
    
    t3 = PythonOperator(
        task_id="case_3",
        python_callable=run_adhoc_test_logic,
        op_args=["42f4589a-05b2-493b-8810-a2bb6c7c1232"],
    )
    
    
    t1 >> t2
    
    t2 >> t3
    