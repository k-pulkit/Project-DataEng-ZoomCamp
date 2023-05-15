import os
import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.email_operator import EmailOperator
from airflow.utils.edgemodifier import Label

from google.cloud import storage
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator
import pyarrow.csv as pv
import pyarrow.parquet as pq

# This script will run inside airflow worker, and we have defined some ENV variables while defining airflow image

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET_NAME = os.environ.get("GCP_GCS_BUCKET")

URL = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_2019-01.csv.gz"

dataset_file = "yellow_tripdata_2019-01.csv.gz"
dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/{dataset_file}"
path_to_local_home = os.environ.get("AIRFLOW_HOME", "/opt/airflow")
parquet_file = dataset_file.replace("csv.gz", "parquet")
BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", "trips_data_all")

def s_f_call(context, msg):
    ti = context["task_instance"]
    task_id = ti.task_id
    execution_date = ti.execution_date
    print(f"Task {task_id} {msg} {execution_date}")

def success_call(context):
    s_f_call(context, "succeeded on")

def failure_call(context):
    s_f_call(context, "failed on")

default_args={
        "depends_on_past": False,
        "email": ["airflow@example.com"],
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
        'on_failure_callback': failure_call, 
        'on_success_callback': success_call, 
        # 'end_date': datetime(2016, 1, 1),
        # 'wait_for_downstream': False,
        # 'sla': timedelta(hours=2),
        # 'trigger_rule': 'all_success'
    }

with DAG(
    "MyPipeline",
    default_args=default_args,
    description = "Pipeline to ingest NYtaxi data",
    schedule_interval = '@monthly',
    start_date = datetime(2022, 1, 1),
    catchup = False,
) as dag:
    start_task = DummyOperator(task_id='Start')
    download_task = BashOperator(task_id='Download',
                                 bash_command = f'wget -O {dataset_file} {dataset_url}',
                                 cwd = path_to_local_home)
    transform_task = DummyOperator(task_id='Transform')
    
    # Parallel tasks
    upload_csv_task = DummyOperator(task_id='GCS_CSV_UPLOAD')
    upload_pq_task = DummyOperator(task_id='GCS_PARQUET_UPLOAD')
    upload_bq_task = DummyOperator(task_id='BQ_UPLOAD')
    
    # Branching
    def check_success(**context):
        parallel_task_ids = ["GCS_CSV_UPLOAD", "GCS_PARQUET_UPLOAD", "BQ_UPLOAD"]
        task_instances = context["task_instance"]
        return "CLEANUP_LOCALFILES"
    
    check_success_task = BranchPythonOperator(task_id="Check_success_upstream", python_callable=check_success, provide_context=True)
    
    # success task
    delete_localfile_task = DummyOperator(task_id='CLEANUP_LOCALFILES')
    
    # Send email of failure task
    notify = EmailOperator(task_id="send_email",
                           to = "pulkit42041@gmail.com",
                           subject='Airflow Email Example',
                           html_content='<p>The execution of the Airflow dag failed. DAG id is {{ dag_run.run_id }} on date {{ dag_run.execution_date }}.</p>',
        )
    
    # End task
    end_task = DummyOperator(task_id='End', trigger_rule='none_failed')
    
    start_task >> download_task >> transform_task >> [upload_bq_task, upload_csv_task, upload_pq_task] >> check_success_task 
    check_success_task >> Label("All tasks success") >> delete_localfile_task >> end_task
    check_success_task >> Label("NOT success") >> notify >> end_task
    
    
    
    




