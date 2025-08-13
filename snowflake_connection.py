from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
import snowflake.connector

# --- Enter your Snowflake credentials here ---
SNOWFLAKE_CREDENTIALS = {
    "user": "YOUR_USERNAME",
    "password": "YOUR_PASSWORD",
    "account": "YOUR_ACCOUNT",  # e.g. 'xy12345.us-east-1'
    "warehouse": "YOUR_WAREHOUSE",
    "database": "YOUR_DATABASE",
    "schema": "YOUR_SCHEMA",
    "role": "YOUR_ROLE"
}

# --- Define your test query ---
TEST_QUERY = """
SELECT TEST_GROUP_ID, GROUP_NAME, SCHEDULE_CRON, CREATED_AT
FROM test_groups
ORDER BY created_at DESC
LIMIT 1000;
"""

def test_snowflake_fetch():
    creds = SNOWFLAKE_CREDENTIALS
    try:
        conn = snowflake.connector.connect(
            user=creds["user"],
            password=creds["password"],
            account=creds["account"],
            warehouse=creds["warehouse"],
            database=creds["database"],
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
    dag_id="test_snowflake_direct_creds",from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
import snowflake.connector

# --- Directly include your credentials ---
DB_CREDENTIALS = {
    "snowflake_dev": {
        "type": "snowflake",
        "username": "dev_ds",
        "password": "Devds@2022",
        "company": "marico-analytics",
        "warehouse": "PRD_BI_COMPUTE_WHS",
        "db": "DEV_DB",
        "schema": "DATA_SCIENCE",
        "role": "DEV_DATA_SCIENTIST"
    }
}

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
    dag_id="test_snowflake_with_inline_creds",
    schedule_interval=None,  # Replace as needed
    start_date=datetime(2023, 1, 1),
    catchup=False,
) as dag:
    fetch_task = PythonOperator(
        task_id="fetch_test_groups",
        python_callable=test_snowflake_fetch,
    )

    schedule_interval=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
) as dag:
    fetch_task = PythonOperator(
        task_id="fetch_test_groups",
        python_callable=test_snowflake_fetch,
    )
