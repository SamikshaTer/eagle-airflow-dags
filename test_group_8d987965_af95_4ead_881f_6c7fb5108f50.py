
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime


def run_adhoc_test_logic(test_case_id):
    print(f"Executing test case: {test_case_id}")

with DAG(
    dag_id="test_group_8d987965_af95_4ead_881f_6c7fb5108f50",
    schedule_interval="56 10 * * *",
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
        op_args=["42f4589a-05b2-493b-8810-a2bb6c7c1232"],
    )
    
    t3 = PythonOperator(
        task_id="case_3",
        python_callable=run_adhoc_test_logic,
        op_args=["604f5064-cb84-44ae-a4cf-db870ba8ccfb"],
    )
    
    
    t1 >> t2
    
    t2 >> t3
    