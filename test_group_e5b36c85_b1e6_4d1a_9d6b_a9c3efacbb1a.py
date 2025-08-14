
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import uuid
import logging
import json
import warnings
import sys

# Correct imports from your new dq_engine package
from dq_engine.config import DB_CREDENTIALS
from dq_engine.dq_core import get_db_connection, log_test_case_result
from dq_engine.services import get_test_case_details_from_db, _extract_and_validate_form_data
from dq_engine.test_case_manager import TestCaseProcessor

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

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

with DAG(
    dag_id="test_group_e5b36c85_b1e6_4d1a_9d6b_a9c3efacbb1a",
    schedule_interval="59 10 * * *",
    start_date=datetime(2023, 1, 1),
    catchup=False,
) as dag:
    
    t1 = PythonOperator(
        task_id="case_1",
        python_callable=run_adhoc_test_logic,
        op_kwargs={'test_case_id': "3e598c0c-3a99-4127-ae1a-11c48750f289"},
    )
    
    t2 = PythonOperator(
        task_id="case_2",
        python_callable=run_adhoc_test_logic,
        op_kwargs={'test_case_id': "f225606b-21c0-4e82-b978-12c4cb77f161"},
    )
    
    t3 = PythonOperator(
        task_id="case_3",
        python_callable=run_adhoc_test_logic,
        op_kwargs={'test_case_id': "41501c34-e46f-47f9-9fb8-42825514f6ad"},
    )
    
    t4 = PythonOperator(
        task_id="case_4",
        python_callable=run_adhoc_test_logic,
        op_kwargs={'test_case_id': "42f4589a-05b2-493b-8810-a2bb6c7c1232"},
    )
    
    t5 = PythonOperator(
        task_id="case_5",
        python_callable=run_adhoc_test_logic,
        op_kwargs={'test_case_id': "604f5064-cb84-44ae-a4cf-db870ba8ccfb"},
    )
    
    
    t1 >> t2
    
    t2 >> t3
    
    t3 >> t4
    
    t4 >> t5
    