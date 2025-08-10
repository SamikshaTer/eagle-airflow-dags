
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def run_adhoc_test_logic(test_case_id):
    print(f"Executing test case: {test_case_id}")

with DAG(
    dag_id="test_group_9a9c7a8c_58e3_46d7_87fb_b84ac68cc5fa",
    schedule=None,
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
    
    
    t1 >> t2
    