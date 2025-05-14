import os
from pathlib import Path
import datetime

from typing import Callable
import uuid

from airflow import models

from airflow.models.baseoperator import chain
from airflow.models.dag import DAG

from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

from airflow.providers.google.cloud.hooks.dataflow import DataflowJobStatus

from airflow.providers.google.cloud.operators.dataflow import (
    DataflowStartFlexTemplateOperator,
    DataflowTemplatedJobStartOperator,
)
from airflow.providers.google.cloud.sensors.dataflow import (
    DataflowJobAutoScalingEventsSensor,
    DataflowJobMessagesSensor,
    DataflowJobMetricsSensor,
    DataflowJobStatusSensor,
)

PROJECT_ID = "v3-playground-366213"

default_dag_args = {
    "start_date": datetime.datetime(2025, 5, 8),
}

LOCATION = "us"

job_name = "transfer"+datetime.datetime.now().strftime("%Y%m%d%H%M%S")

FlexTemplateBody = {
    "launchParameter": {
        "jobName": job_name,
        "parameters": {
            "mongoDbUri": "mongodb+srv://bigquery:simplePassword@clusterenvision.bwqtr.mongodb.net/",
            "database": "ETS",
            "collection": "Records6521",
            "userOption": "JSON",
            "useStorageWriteApi": "false",
            "outputTableSpec": "v3-playground-366213:ETS.ServiceItems"
        },
        "environment": {
            "additionalExperiments": [],
            "additionalUserLabels": {}
        },
        "containerSpecGcsPath": "gs://dataflow-templates-us-central1/latest/flex/MongoDB_to_BigQuery",
    }
}

# Define a DAG (directed acyclic graph) of tasks.
# Any task you create within the context manager is automatically added to the
# DAG object.
with models.DAG(
    "sincronize_mongodbcollection_test",
    schedule_interval=datetime.timedelta(days=1),
    default_args=default_dag_args,
) as dag:
    
    truncate_table_operator = BigQueryInsertJobOperator(
       task_id="truncate_table",
        configuration={
            "query": {
                "query": f"truncate table`{PROJECT_ID}.ETS.Logs`",
                "useLegacySql": False,
                "priority": "BATCH",
            }
        },
        location=LOCATION
    )    
    
    start_template_job = DataflowStartFlexTemplateOperator(
        task_id="dataflow_import",
        project_id=PROJECT_ID,
        body=FlexTemplateBody,
        append_job_name=False,
        location=LOCATION,
        wait_until_finished=False
    )        
    
    wait_for_job = DataflowJobStatusSensor(
        task_id="wait_for_templatejob",
        job_id="{{task_instance.xcom_pull('dataflow_import', key='job_id')}}",
        expected_statuses=DataflowJobStatus.JOB_STATE_DONE,
        deferrable=True,
        location=LOCATION
    )

    
    insert_query_job = BigQueryInsertJobOperator(
       task_id="insert_query_job",
        configuration={
            "query": {
                "query": f"insert into `{PROJECT_ID}.ETS.Logs`(DateTime,Text) values(current_datetime(),'finised importing')",
                "useLegacySql": False,
                "priority": "BATCH",
            }
        },
        location=LOCATION
    )
    
    truncate_table_operator >> start_template_job >> wait_for_job >> insert_query_job
    
    
