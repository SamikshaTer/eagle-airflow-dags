
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def run_adhoc_test_logic(test_case_id):
    print(f"Executing test case: {test_case_id}")

with DAG(
    dag_id="test_group_9d08e4c9_94b0_46a7_b7b3_0c617a8e9a09",
    schedule=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
) as dag:
    
    t1 = PythonOperator(
        task_id="case_1",
        python_callable=run_adhoc_test_logic,
        op_args=["41501c34-e46f-47f9-9fb8-42825514f6ad"],
    )
    
    t2 = PythonOperator(
        task_id="case_2",
        python_callable=run_adhoc_test_logic,
        op_args=["f225606b-21c0-4e82-b978-12c4cb77f161"],
    )
    
    t3 = PythonOperator(
        task_id="case_3",
        python_callable=run_adhoc_test_logic,
        op_args=["3e598c0c-3a99-4127-ae1a-11c48750f289"],
    )
    
    t4 = PythonOperator(
        task_id="case_4",
        python_callable=run_adhoc_test_logic,
        op_args=["42f4589a-05b2-493b-8810-a2bb6c7c1232"],
    )
    
    
    t1 >> t2
    
    t2 >> t3
    
    t3 >> t4
    