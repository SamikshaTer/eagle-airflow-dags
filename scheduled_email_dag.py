# import pendulum
# from airflow.models.dag import DAG
# from airflow.operators.python import PythonOperator
# from airflow.operators.email import EmailOperator
# from airflow.exceptions import AirflowFailException

# # 1. EDIT THIS: The email address to send alerts to.
# MANAGER_EMAIL = "samiksha.terwankar@marico.com"

# # 2. EDIT THIS: The link in the email. Use your server's public IP.
# FAILURE_EMAIL_HTML = f"""
# <h3>Data Match Test Failed!</h3>
# <p>A data mismatch was found during the scheduled run.</p><br>
# <a href="http://10.124.10.138:3000?run_id={{ run_id }}"
#    style="background-color: #d9534f; color: white; padding: 14px 25px; text-decoration: none; display: inline-block; border-radius: 5px;">
#    Go to Dashboard to Rerun
# </a><br><p><small>DAG Run ID: {{ run_id }}</small></p>
# """
# SUCCESS_EMAIL_HTML = "<h3>Data Match Test Passed!</h3><p>No issues found.</p>"

# def run_snowflake_data_match_test():
#     """This function connects to Snowflake and runs the test."""
#     from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
#     hook = SnowflakeHook(snowflake_conn_id="snowflake_demo_conn")
#     sql = "SELECT COUNT(*) FROM EAGLE_DEMO_DB.DEMO_SCHEMA.DATA_MATCH_TEST WHERE SOURCE_DATA <> DESTINATION_DATA;"

#     print(f"Running query: {sql}")
#     result = hook.get_first(sql)
#     mismatch_count = result[0]

#     if mismatch_count > 0:
#         raise AirflowFailException(f"Test Failed! Found {mismatch_count} mismatches.")
#     else:
#         print("Test Passed!")

# with DAG(
#     dag_id="scheduled_email_dag",
#     schedule="*/5 * * * *",  # Runs every 5 minutes for this demo
#     start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
#     catchup=False,
# ) as dag:
#     run_test = PythonOperator(
#         task_id="run_snowflake_data_match_test",
#         python_callable=run_snowflake_data_match_test,
#     )
#     send_success_email = EmailOperator(
#         task_id="send_success_email", to=MANAGER_EMAIL, subject="Airflow: [SUCCESS] Data Match Test",
#         html_content=SUCCESS_EMAIL_HTML, trigger_rule="all_success",
#     )
#     send_failure_email = EmailOperator(
#         task_id="send_failure_email", to=MANAGER_EMAIL, subject="Airflow: [FAILURE] Data Match Test",
#         html_content=FAILURE_EMAIL_HTML, trigger_rule="one_failed",
#     )
#     run_test >> [send_success_email, send_failure_email]

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
 
def run_task():
    print("Running email DAG")
 
with DAG(
    dag_id="scheduled_email_dag",  # This will show in the UI
    schedule_interval=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
) as dag:
    task = PythonOperator(
        task_id='run_task',
        python_callable=run_task,
    )