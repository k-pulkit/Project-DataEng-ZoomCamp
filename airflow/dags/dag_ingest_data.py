import os
import subprocess
import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.email_operator import EmailOperator
from airflow.utils.edgemodifier import Label
from airflow.decorators import task
from airflow.operators.python import get_current_context

from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator
from airflow.contrib.operators.bigquery_operator import BigQueryExecuteQueryOperator
import pyarrow.csv as pycsv
import pyarrow.parquet as pypq

# This script will run inside airflow worker, and we have defined some ENV variables while defining airflow image

# PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
# BUCKET_NAME = os.environ.get("GCP_GCS_BUCKET")

PROJECT_ID = Variable.get("GCP_PROJECT_ID", default_var="dezoomcamp-384523")
BUCKET_NAME = Variable.get("GCP_GCS_BUCKET", default_var="dezcamp_zucket_dezoomcamp-384523")

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
        "retry_delay": timedelta(minutes=1),
        'on_failure_callback': failure_call, 
        'on_success_callback': success_call, 
        # 'end_date': datetime(2016, 1, 1),
        # 'wait_for_downstream': False,
        # 'sla': timedelta(hours=2),
        # 'trigger_rule': 'all_success'
    }

def create_dag(taxi_type, start, end):
    
    taxi_type=taxi_type

    dag = DAG(
        f"Ingestion_Pipeline_{taxi_type}",
        default_args=default_args,
        description = "Pipeline to ingest NYtaxi data",
        schedule_interval = '@monthly',
        start_date = start,
        end_date=end,
        catchup = True,
        max_active_runs = 1,
        dagrun_timeout=timedelta(minutes=40)
    ) 

    with dag:
        
        # Dataset filename created dynamically using the airflow execution date
        
        path_to_local_home = os.environ.get("AIRFLOW_HOME", "/opt/airflow")
        BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", "trips_data_all")
        
        
        start_task = DummyOperator(task_id='Start')
        
        @task(task_id='GetDownloadFileInfo')
        def file_info():
            context = get_current_context()
            execution_date = context["execution_date"]
            dataset_file = taxi_type + f'_tripdata_{execution_date.strftime("%Y-%m")}.csv.gz'
            dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{taxi_type}/{dataset_file}"
            parquet_file = dataset_file.replace("csv.gz", "parquet")
            # GCS PREFIX
            base_prefix = 'tripsdata'
            bucket_prefix = f"{base_prefix}/taxitype={taxi_type}" + f'/year={execution_date.strftime("%Y")}/month={execution_date.strftime("%m")}'
            csv_obj_name = f"csv/{bucket_prefix}/{dataset_file}"
            parquet_obj_name = f"parquet/{bucket_prefix}/{parquet_file}"
            # Push to xcom
            context['ti'].xcom_push(key='dataset_file', value=dataset_file)
            context['ti'].xcom_push(key='dataset_url', value=dataset_url)
            context['ti'].xcom_push(key='parquet_file', value=parquet_file)
            context['ti'].xcom_push(key='csv_obj_name', value=csv_obj_name)
            context['ti'].xcom_push(key='parquet_obj_name', value=parquet_obj_name)
        
        get_file_info = file_info()
        
        @task(task_id='Download')
        def download_file():
            context = get_current_context()
            dataset_file = context['ti'].xcom_pull(key='dataset_file', task_ids="GetDownloadFileInfo")
            dataset_url = context['ti'].xcom_pull(key='dataset_url', task_ids="GetDownloadFileInfo")
            csv_obj_name = context['ti'].xcom_pull(key='csv_obj_name', task_ids="GetDownloadFileInfo")
            # download file
            bash_command = f'wget -O {path_to_local_home}/{dataset_file} {dataset_url}'
            os.system(bash_command)
            # upload to GCS bucket temp location
            gcs_hook = GCSHook(gcp_conn_id='google_cloud_default')
            gcs_hook.upload(
                bucket_name=BUCKET_NAME,
                object_name=csv_obj_name,
                filename=f'{path_to_local_home}/{dataset_file}'
            )
            print("File uploaded to GCS")
            # Delete from local
            os.system(f'rm -f {path_to_local_home}/{dataset_file}')
            # return
            return csv_obj_name
            
        download_task = download_file()
        
        @task(task_id='Transform')
        def convert_to_parquet():
            context = get_current_context()
            gcp_object = context.get("task_instance").xcom_pull(key='return_value', task_ids='Download')
            dataset_file = context['ti'].xcom_pull(key='dataset_file', task_ids="GetDownloadFileInfo")
            parquet_file = context['ti'].xcom_pull(key='parquet_file', task_ids="GetDownloadFileInfo")
            parquet_obj_name = context['ti'].xcom_pull(key='parquet_obj_name', task_ids="GetDownloadFileInfo")
            # Download GCP file
            gcs_hook = GCSHook(gcp_conn_id='google_cloud_default')
            gcs_hook.download(
                bucket_name=BUCKET_NAME,
                object_name=gcp_object,
                filename=f'{path_to_local_home}/{dataset_file}'
            )
            # convert the file
            pytable = pycsv.read_csv(f"{path_to_local_home}/{dataset_file}")
            pypq.write_table(pytable, f"{path_to_local_home}/{parquet_file}", compression="snappy")
            # Upload to GCS
            gcs_hook.upload(
                bucket_name=BUCKET_NAME,
                object_name=parquet_obj_name,
                filename=f'{path_to_local_home}/{parquet_file}'
            )
            # Delete file from local
            os.system(f'rm -f {path_to_local_home}/{dataset_file}')
            os.system(f'rm -f {path_to_local_home}/{parquet_file}')
            # Return paths of files
            return parquet_obj_name
        
        transform_task = convert_to_parquet()
        
        # BQ task
        upload_bq_task = BigQueryCreateExternalTableOperator(task_id='BQ_ExternalTable',
                                                            table_resource={
                                                                "tableReference": {
                                                                    "projectId": PROJECT_ID,
                                                                    "datasetId": BIGQUERY_DATASET,
                                                                    "tableId": f"ExternalTable_{taxi_type}"
                                                                },
                                                                "externalDataConfiguration": {
                                                                    "sourceFormat": "PARQUET",
                                                                    "sourceUris": [f"gs://{BUCKET_NAME}/parquet/tripsdata/taxitype={taxi_type}/*"]
                                                                }
                                                            }
                                                            )
        
        # Branching
        def check_success(**context):
            parallel_task_ids = ["GCS_CSV_UPLOAD", "GCS_PARQUET_UPLOAD", "BQ_UPLOAD"]
            task_instances = context["task_instance"]
            return "End"
        
        check_success_task = BranchPythonOperator(task_id="Check_success_upstream", python_callable=check_success, provide_context=True)

        # Send email of failure task
        notify = EmailOperator(task_id="send_email",
                            to = "pulkit42041@gmail.com",
                            subject='Airflow Email Example',
                            html_content='<p>The execution of the Airflow dag failed. DAG id is {{ dag_run.run_id }} on date {{ dag_run.execution_date }}.</p>',
            )
        
        # End task
        end_task = DummyOperator(task_id='End', trigger_rule='none_failed')
        
        start_task >> get_file_info >> download_task >> transform_task >> upload_bq_task
        upload_bq_task >> check_success_task 
        check_success_task >> Label("Success") >> end_task
        check_success_task >> notify >> end_task
    
    return dag

#dag_yellow = create_dag("yellow", datetime(2019, 1, 1), datetime(2021, 7, 1))
#dag_fhv = create_dag("fhv", datetime(2019, 1, 1), datetime(2021, 7, 1))
dag_green = create_dag("green", datetime(2019, 1, 1), datetime(2021, 7, 1))
        
        
        
        




