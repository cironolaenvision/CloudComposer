import os
from pathlib import Path
import datetime
import string

from typing import Callable
import uuid

from google.cloud import bigquery

import requests

# import the logging module
import logging

# get the airflow.task logger
task_logger = logging.getLogger("airflow.task")

from airflow import models

from airflow.providers.standard.operators.python import PythonOperator

from airflow.models.param import Param

from airflow.models.baseoperator import chain, chain_linear
from airflow.decorators import(dag, task,task_group) #foi

from airflow.sensors.base import (PokeReturnValue)

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
import json

PROJECT_ID = "v3-playground-366213"
DATASET = "ETS"

dag_args = {
    "start_date": datetime.datetime(2025, 5, 8)
}

LOCATION = "us"

data_dir = os.path.dirname(os.path.abspath(__file__)).replace('dags','data')

def GetAllCollections():
    file = open(os.path.join(data_dir,dag.params["tables_path"]))
    decoded = json.loads(file.read())
    accounts = []
    
    for account in decoded:
        account["IdName"] = str(account["Id"])+"_"+account["AccountName"]
        account["CollectionName"] = f"Records{account['Id']}"
        accounts.append(account)
        
    return accounts
    

with models.DAG(
    "mongodb_tobigquery_transfer",
    schedule_interval=None,
    default_args=dag_args,
    params= {
        "tables_path": "all_collections.json",
        "chunk_size": 10
    }
) as dag:
    
    @task_group
    def OneCollectionTransfer(account):
                    
        @task(task_id=f"prepTable")
        def prepare_destination_table(collectionName):
            task_logger.info(f"Preparing table {collectionName}")
            client = bigquery.Client()
            
            client.query(f"call `ETS.create_or_truncate_table`(Records'{collectionName}');")
        
        prep_table = prepare_destination_table(account["CollectionName"])
        
        transfer = DataflowStartFlexTemplateOperator(
            task_id=f"transfer",
            project_id=PROJECT_ID,
            append_job_name=False,
            location=LOCATION,
            wait_until_finished=False,
            body={
                "launchParameter": {
                    "jobName": f"transfer_{account["CollectionName"].lower()}_{datetime.datetime.now().strftime('%Y%m%d%H%M%S')}",
                    "parameters": {
                        "mongoDbUri": "mongodb+srv://bigquery:simplePassword@clusterenvision.bwqtr.mongodb.net/",
                        "database": "ETS",
                        "collection": account["CollectionName"],
                        "userOption": "JSON",
                        "useStorageWriteApi": "false",
                        "outputTableSpec": f"v3-playground-366213:ETS.{account['CollectionName']}"
                    },
                    "environment": {
                        "additionalExperiments": [],
                        "additionalUserLabels": {}
                    },
                    "containerSpecGcsPath": "gs://dataflow-templates-us-central1/latest/flex/MongoDB_to_BigQuery"
                }
            }
        )

        wait_for_transfer = DataflowJobStatusSensor(
            task_id="wait_for_templatejob",
            job_id="{{task_instance.xcom_pull('"+account['IdName']+".transfer', key='job_id')}}",
            expected_statuses=DataflowJobStatus.JOB_STATE_DONE,
            deferrable=True,
            location=LOCATION
        )       
        
        insert = BigQueryInsertJobOperator(
            task_id="insert_query_job",
            configuration={
                "query": {
                    "query": f"insert into `{PROJECT_ID}.ETS.Logs`(DateTime,Text) values(current_datetime(),'finised importing - {account['AccountName']} ')",
                    "useLegacySql": False,
                    "priority": "BATCH",
                }
            },
            project_id=PROJECT_ID,
            location=LOCATION          
        )         
        
        prep_table >> transfer >> wait_for_transfer >> insert

    @task
    def ClusterCollection(account):
        task_logger.info(f"Clustering table {account['AccountName']}")
        client = bigquery.Client()
        client.query(f"call `ETS.cluster_data`('{account['Id']}','{account['AccountName']}');")      
        client.query(f"insert into `{PROJECT_ID}.ETS.Logs`(DateTime,Text) values(current_datetime(),'finised clustering - {account['AccountName']} ')")
        client.close()
    
    @task
    def PrintCollectionNames(collection):
        for i in range(0,len(collection)):
            task_logger.info(f"PRINT - Index: {i}, Value: {collection[i]['IdName']}")
            
    @task(trigger_rule="one_success")
    def WaitOnCollections(collection):
        task_logger.info(f"Just waiting done - {collection}")
    
    accounts = GetAllCollections()
    localList = []
    localGroups = []

    i=1
    end = dag.params["chunk_size"]
    lastWait = None
    while(i <= len(accounts)):
        account = accounts[i-1]
        
        oneTransfer = OneCollectionTransfer.override(group_id=account["IdName"])(account)
        localList.append(account)
        localGroups.append(oneTransfer)
        
        if(i % dag.params["chunk_size"] == 0):
            print = PrintCollectionNames(localList)
            if(lastWait):
                print.set_upstream(lastWait)
                
            for group in localGroups:
                group.set_upstream(print)
            
            for y, a in enumerate(localList):
                cluster = ClusterCollection.override(task_id=f"Cluster_{a['IdName']}")(a)
                cluster.set_upstream(localGroups[y])
                
            wait = WaitOnCollections(localList)
            lastWait = wait
            for group in localGroups:
                wait.set_upstream(group)
                
            localList = []
            localGroups = []
                           
        i = i + 1
    
    if(len(localGroups) > 0):
        print = PrintCollectionNames(localList)
        if(lastWait):
            print.set_upstream(lastWait)
            
        for group in localGroups:
            group.set_upstream(print)
        
        for y, a in enumerate(localList):
            cluster = ClusterCollection.override(task_id=f"Cluster_{a['IdName']}")(a)
            cluster.set_upstream(localGroups[y])
            
        wait = WaitOnCollections(localList)
        lastWait = wait
        for group in localGroups:
            wait.set_upstream(group)
            
        localList = []
        localGroups = []
        
        
        
    