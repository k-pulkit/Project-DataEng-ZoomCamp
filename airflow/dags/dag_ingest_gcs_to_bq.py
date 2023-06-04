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
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator, BigQueryInsertJobOperator

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

def create_dag(start, end):
    
    #taxi_types = ["yellow", "green", "fhv"]
    taxi_types = ["green", "yellow", "fhv"]
    partition_cols = {
        "yellow": "tpep_pickup_datetime",
        "green": "lpep_pickup_datetime",
        "fhv": "pickup_datetime"
    }

    dag = DAG(
        f"dag_GCS_to_BQ",
        default_args=default_args,
        description = "Pipeline to move data from gcs to bq",
        schedule_interval = None,
        start_date = start,
        catchup = False,
        max_active_runs = 1,
        dagrun_timeout=timedelta(minutes=40)
    ) 

    with dag:
        
        # Dataset filename created dynamically using the airflow execution date
        
        path_to_local_home = os.environ.get("AIRFLOW_HOME", "/opt/airflow")
        BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", "trips_data_all")
        
        start_task = DummyOperator(task_id='Start')
        
        # End task
        end_task = DummyOperator(task_id='End', trigger_rule='none_failed')
        
        for taxi_type in taxi_types:
         
            # BQ task
            upload_bq_task = BigQueryCreateExternalTableOperator(task_id=f'BQ_ExternalTable_{taxi_type}',
                                                                table_resource={
                                                                    "tableReference": {
                                                                        "projectId": PROJECT_ID,
                                                                        "datasetId": BIGQUERY_DATASET,
                                                                        "tableId": f"ExternalTable_{taxi_type}"
                                                                    },
                                                                    "externalDataConfiguration": {
                                                                        "sourceFormat": "PARQUET",
                                                                        "sourceUris": [f"gs://{BUCKET_NAME}/raw_parquet/tripsdata/taxitype={taxi_type}/*"],
                                                                        "hivePartitioningOptions": {
                                                                            "mode": "AUTO",
                                                                            "sourceUriPrefix": f"gs://{BUCKET_NAME}/raw_parquet/tripsdata/",
                                                                            "requirePartitionFilter": False,
                                                                            "fields": [
                                                                                "taxitype", "year"
                                                                            ]
                                                                        }
                                                                    }
                                                                }
                                                                )
            
            # Create partitined table using external table
            condition = taxi_type != "fhv"
            QUERY = f"""
            CREATE OR REPLACE TABLE `{PROJECT_ID}.{BIGQUERY_DATASET}.{taxi_type}_partitoned_clustered`
            PARTITION BY DATE({partition_cols[taxi_type]}) """+\
            """{% if params.condition %}
            CLUSTER BY VendorID 
            {% endif %}
            """+\
            "AS SELECT * {% if params.is_green_taxi %} EXCEPT (ehail_fee)  {% endif %}" + f"FROM `{PROJECT_ID}.{BIGQUERY_DATASET}.ExternalTable_{taxi_type}`"
            
            create_native_table_task = BigQueryInsertJobOperator(task_id=f"create_native_bq_table_{taxi_type}",
                                                                configuration={
                                                                "query": {
                                                                    "query": QUERY,
                                                                    "useLegacySql": False
                                                                }
                                                                },
                                                                params={
                                                                    "condition": condition,
                                                                    "is_green_taxi": taxi_type=="green"
                                                                }
                                                                )
            start_task >> upload_bq_task >> create_native_table_task >> end_task
    
    return dag

dag_gcs_to_bq = create_dag(datetime(2019, 1, 1), datetime(2021, 7, 1))
        
        
        
        




