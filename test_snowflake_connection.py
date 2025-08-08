from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from datetime import datetime


def test_snowflake_conn():
    try:
        conn_id = "snowflake_demo_conn"
        hook = SnowflakeHook(snowflake_conn_id=conn_id)
        conn = hook.get_conn()
        cursor = conn.cursor()
        cursor.execute("SELECT CURRENT_VERSION()")
        result = cursor.fetchone()
        print("✅ Connection succeeded. Snowflake version:", result[0])
        cursor.close()
        conn.close()
    except Exception as e:
        raise RuntimeError(f"❌ Connection failed: {str(e)}")


with DAG(
    dag_id="test_snowflake_connection_dag",
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,  # Manual trigger
    catchup=False,
    tags=["snowflake", "test"],
) as dag:

    test_connection_task = PythonOperator(
        task_id="test_snowflake_connection",
        python_callable=test_snowflake_conn,
    )
