
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pendulum

def run_adhoc_test_logic(test_case_id):
    print(f"Executing test case: {test_case_id}")

with DAG(
    dag_id="test_group_fcd3d6bb_c5ba_4663_aa29_f41e1e682f49",
    schedule_interval="36 13 * * *",
    start_date=pendulum.datetime(2023, 1, 1, tz="Asia/Kolkata"),
    timezone="Asia/Kolkata",
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
    