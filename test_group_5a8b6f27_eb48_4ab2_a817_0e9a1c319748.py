
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime


def run_adhoc_test_logic(test_case_id):
    print(f"Executing test case: {test_case_id}")

with DAG(
    dag_id="test_group_5a8b6f27_eb48_4ab2_a817_0e9a1c319748",
    schedule_interval="*/5 * * * *",
    start_date=datetime(2023, 1, 1),
    catchup=False,
) as dag:
    
    t1 = PythonOperator(
        task_id="case_1",
        python_callable=run_adhoc_test_logic,
        op_args=["3e598c0c-3a99-4127-ae1a-11c48750f289"],
    )
    
    