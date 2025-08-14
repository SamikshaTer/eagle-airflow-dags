
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import uuid
import logging
import json
import warnings
import sys
import pandas as pd

# Correct imports from your new dq_engine package
from dq_engine.config import DB_CREDENTIALS
from dq_engine.dq_core import get_db_connection, execute_query_to_dataframe, log_test_case_result, log_group_run_status
from dq_engine.services import get_test_case_details_from_db, _extract_and_validate_form_data, get_test_cases_in_group_from_db
from dq_engine.test_case_manager import TestCaseProcessor

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# NEW FUNCTION: This task creates the initial group log record.
def start_group_logging(test_group_id, **kwargs):
    metadata_conn_name = 'snowflake_dev'
    db_credentials = DB_CREDENTIALS
    parent_run_id = kwargs.get('dag_run').run_id
    
    conn = get_db_connection(metadata_conn_name, db_credentials)
    if not conn:
        logger.error("Failed to connect to metadata DB for initial logging.")
        return

    try:
        log_group_run_status(
            log_conn=conn,
            run_id=parent_run_id,
            test_group_id=test_group_id,
            status='RUNNING',
            message='Test group run started.',
            results_details={},
            start_time=datetime.now()
        )
    except Exception as e:
        logger.exception("An error occurred during initial group status logging.")
    finally:
        if conn: conn.close()


# This is the new, complete function that will run on the Airflow worker
def run_adhoc_test_logic(test_case_id, **kwargs):
    metadata_conn_name = 'snowflake_dev'
    db_credentials = DB_CREDENTIALS
    parent_run_id = kwargs.get('dag_run').run_id
    
    logger.info(f"Starting Airflow execution for test case: {test_case_id}")
    
    test_processor = TestCaseProcessor(db_credentials)
    run_id = str(uuid.uuid4())

    result = {
        "status": "ERROR",
        "message": "Test execution failed unexpectedly or was not fully processed.",
        'run_id': run_id,
        'test_case_id': test_case_id,
        'project_id': None,
        'test_name': None
    }
    
    try:
        test_case_data = get_test_case_details_from_db(test_case_id, metadata_conn_name, db_credentials)
        if not test_case_data:
            result['message'] = "Test case not found in metadata database."
            return
        
        result['project_id'] = test_case_data.get('project_id')
        result['test_name'] = test_case_data.get('test_name')
        
        config, validation_errors = _extract_and_validate_form_data(test_case_data, db_credentials)
        
        if validation_errors:
            result['message'] = f"Validation errors: {validation_errors}"
        else:
            processor_result = test_processor.process_test_request(config, action='run', run_id=run_id)
            result.update(processor_result)

    except Exception as e:
        result['message'] = f"An unhandled error occurred: {e}"
        logger.exception("An unhandled error occurred during Airflow test execution.")

    finally:
        log_conn = get_db_connection(metadata_conn_name, db_credentials)
        if log_conn:
            log_test_case_result(
                log_conn=log_conn,
                run_id=result.get('run_id'),
                test_case_id=result.get('test_case_id'),
                project_id=result.get('project_id'),
                test_name=result.get('test_name'),
                run_status=result.get('status'),
                run_message=result.get('message'),
                source_value=result.get('source_value'),
                destination_value=result.get('destination_value'),
                difference=result.get('difference'),
                threshold_applied={'type': result.get('threshold_type'), 'value': result.get('threshold')},
                source_query=result.get('source_query'),
                destination_query=result.get('destination_query'),
                source_connection=result.get('source_connection_used'),
                destination_connection=result.get('destination_connection_used'),
                parent_run_id=parent_run_id
            )
            log_conn.close()

# NEW FUNCTION: This function logs the final status of the entire group
def log_final_group_status(test_group_id, **kwargs):
    metadata_conn_name = 'snowflake_dev'
    db_credentials = DB_CREDENTIALS
    parent_run_id = kwargs.get('dag_run').run_id
    
    logger.info(f"Logging final status for group {test_group_id} with parent run ID: {parent_run_id}")
    
    conn = get_db_connection(metadata_conn_name, db_credentials)
    if not conn:
        logger.error("Failed to connect to metadata DB for final logging.")
        return

    try:
        # Fetch all individual test results for this run, including messages
        query = "SELECT TEST_CASE_ID, TEST_NAME, RUN_STATUS, RUN_MESSAGE FROM DEV_DB.PUBLIC.TEST_CASE_LOGS WHERE PARENT_RUN_ID = %s;"
        df, _ = execute_query_to_dataframe(conn, query, params=(parent_run_id,))
        
        # Determine the overall status and count failed tests
        failed_tests = df[df['RUN_STATUS'] == 'FAIL']
        errored_tests = df[df['RUN_STATUS'] == 'ERROR']
        
        failed_count = failed_tests.shape[0] + errored_tests.shape[0]
        
        if errored_tests.shape[0] > 0:
            final_status = 'ERROR'
        elif failed_tests.shape[0] > 0:
            final_status = 'FAIL'
        else:
            final_status = 'PASS'
            
        final_message = f"Group run completed. Overall Status: {final_status}. Failed tests: {failed_count} out of {df.shape[0]}."
        
        # Create the detailed list of failed test cases
        failed_test_cases_list = []
        if failed_count > 0:
            for index, row in pd.concat([failed_tests, errored_tests]).iterrows():
                failed_test_cases_list.append({
                    "test_case_id": row['TEST_CASE_ID'],
                    "test_name": row['TEST_NAME'],
                    "status": row['RUN_STATUS'],
                    "message": row['RUN_MESSAGE'],
                })

        # Build the final results details dictionary
        results_details = {
            "Overall Status": final_status,
            "Total Tests": df.shape[0],
            "Failed Tests": failed_count,
            "Failed Test Cases": failed_test_cases_list
        }

        log_group_run_status(
            log_conn=conn,
            run_id=parent_run_id,
            test_group_id=test_group_id,
            status=final_status,
            message=final_message,
            results_details=results_details,
            start_time=None, # This is an update call
            end_time=datetime.now()
        )
    except Exception as e:
        logger.exception("An error occurred during final group status logging.")
    finally:
        if conn: conn.close()


with DAG(
    dag_id="test_group_568af016_478b_4b06_b651_e1064121d294",
    schedule_interval="35 21 * * *",
    start_date=datetime(2023, 1, 1),
    catchup=False,
) as dag:
    # TASK 1: Start the logging
    start_logging_task = PythonOperator(
        task_id="start_group_logging",
        python_callable=start_group_logging,
        op_kwargs={'test_group_id': '568af016-478b-4b06-b651-e1064121d294'},
    )
    
    # TASKS 2-N: Run all the individual test cases
    test_case_tasks = []
    
    t1 = PythonOperator(
        task_id="case_1",
        python_callable=run_adhoc_test_logic,
        op_kwargs={'test_case_id': "3e598c0c-3a99-4127-ae1a-11c48750f289"},
    )
    test_case_tasks.append(t1)
    
    t2 = PythonOperator(
        task_id="case_2",
        python_callable=run_adhoc_test_logic,
        op_kwargs={'test_case_id': "f225606b-21c0-4e82-b978-12c4cb77f161"},
    )
    test_case_tasks.append(t2)
    
    t3 = PythonOperator(
        task_id="case_3",
        python_callable=run_adhoc_test_logic,
        op_kwargs={'test_case_id': "41501c34-e46f-47f9-9fb8-42825514f6ad"},
    )
    test_case_tasks.append(t3)
    
    t4 = PythonOperator(
        task_id="case_4",
        python_callable=run_adhoc_test_logic,
        op_kwargs={'test_case_id': "42f4589a-05b2-493b-8810-a2bb6c7c1232"},
    )
    test_case_tasks.append(t4)
    
    t5 = PythonOperator(
        task_id="case_5",
        python_callable=run_adhoc_test_logic,
        op_kwargs={'test_case_id': "604f5064-cb84-44ae-a4cf-db870ba8ccfb"},
    )
    test_case_tasks.append(t5)
    
    
    # FINAL TASK: Log the final status of the group, regardless of previous task outcomes
    log_final_status_task = PythonOperator(
        task_id="log_final_group_status",
        python_callable=log_final_group_status,
        op_kwargs={'test_group_id': '568af016-478b-4b06-b651-e1064121d294'},
        trigger_rule='all_done',
    )

    # Set the dependencies
    
    start_logging_task >> test_case_tasks >> log_final_status_task
    