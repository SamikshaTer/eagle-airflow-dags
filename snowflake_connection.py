from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
import snowflake.connector

# --- Import credentials from shared_config ---
from shared_config.config import DB_CREDENTIALS  # Make sure shared_config is in PYTHONPATH

TEST_CONNECTION_NAME = "snowflake_dev"

TEST_QUERY = """
SELECT TEST_GROUP_ID, GROUP_NAME, SCHEDULE_CRON, CREATED_AT
FROM test_groups
ORDER BY created_at DESC
LIMIT 1000;
"""

def test_snowflake_fetch():
    creds = DB_CREDENTIALS[TEST_CONNECTION_NAME]
    try:
        conn = snowflake.connector.connect(
            user=creds["username"],
            password=creds["password"],
            account=creds["company"],
            warehouse=creds["warehouse"],
            database=creds["db"],
            schema=creds["schema"],
            role=creds["role"]
        )
        print("Snowflake connection established successfully.")
        df = pd.read_sql(TEST_QUERY, conn)
        print("\n--- test_groups (First 5 rows) ---")
        print(df.head())
        print(f"\nTotal rows returned: {len(df)}")
        conn.close()
        print("Snowflake connection closed.")
    except Exception as e:
        print(f"ERROR: {e}")
        raise

with DAG(
    dag_id="test_snowflake_with_shared_config",
    schedule_interval=None,  # Replace as needed
    start_date=datetime(2023, 1, 1),
    catchup=False,
) as dag:
    fetch_task = PythonOperator(
        task_id="fetch_test_groups",
        python_callable=test_snowflake_fetch,
    )
