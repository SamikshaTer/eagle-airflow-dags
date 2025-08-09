from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def hello():
    print("Hello from new DAG!")

with DAG(
    dag_id="test_dag",
    schedule_interval=None,  # manual trigger only
    start_date=datetime(2023, 1, 1),
    catchup=False,
) as dag:
    task = PythonOperator(
        task_id="hello_task",
        python_callable=hello,
    )
