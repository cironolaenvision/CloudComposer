import os
from pathlib import Path
import datetime

from typing import Callable
import uuid

from google.cloud import bigquery

import requests

# import the logging module
import logging

# get the airflow.task logger
task_logger = logging.getLogger("airflow.task")

from airflow import models

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

PROJECT_ID = "v3-playground-366213",
DATASET = "ETS"

job_name = "t"+datetime.datetime.now().strftime("%Y%m%d%H%M%S")

FlexTemplateBody = {
    "launchParameter": {
        "jobName": job_name,
        "parameters": {
            "mongoDbUri": "mongodb+srv://bigquery:simplePassword@clusterenvision.bwqtr.mongodb.net/",
            "database": "ETS",
            "collection": "{colName}",
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

FlexTemplateBodyStr =  json.dumps(FlexTemplateBody)

dag_args = {
    "start_date": datetime.datetime(2025, 5, 8),
}

LOCATION = "us"

headers = {
    "Accept": "*/*",
    "Host": "dataflow.googleapis.com",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection" : "keep-alive"
}

tables = ["Records10710","Records3249","Records13976","Records35667","Records3096","Records22092","Records18601","Records2942"]


with models.DAG(
    "transfer_allcollections",
    schedule_interval=None,
    default_args=dag_args,
    params= {
        "numberOfParts": Param(10, type="integer"),
        "accountNames": Param(
            [],
            type="array",
            description="Filter the list of accounts you want to sinc",
            title="Accounts to sync")
    }    
) as dag:
    
    @task_group()
    def OneCollectionTransfer(group_collectionName):
                    
        @task()
        def prepare_destination_table(collectionName):
            task_logger.info(f"Preparing table {collectionName}")
            client = bigquery.Client()

            # Perform a query.
            tables = client.list_tables(DATASET)
            found = False
            for table in tables:
                table.table_id == collectionName
                found = True
                break
            
            if(found == False):
                client.create_table(collectionName)
            else:
                client.query(f"truncate table`{PROJECT_ID}.ETS.{collectionName}`") 
                
            return 1
                
        @task
        def start_templated_tranfer(ret, collectionName):
            task_logger.info(f"Starting transfer for collection - {collectionName}")
            job_name = "col"+datetime.datetime.now().strftime("%Y%m%d%H%M%S")
            body = {
                "launch_parameter": {
                    "jobName": job_name,
                    "containerSpecGcsPath": "gs://dataflow-templates-us-central1/latest/flex/MongoDB_to_BigQuery",
                    "parameters": {
                        "mongoDbUri": "mongodb+srv://bigquery:simplePassword@clusterenvision.bwqtr.mongodb.net/",
                        "database": "ETS",
                        "collection": collectionName,
                        "userOption": "JSON",
                        "useStorageWriteApi": "false",
                        "outputTableSpec": f"v3-playground-366213:ETS.{collectionName}"
                    },
                    "environment": {
                        "additionalExperiments": [],
                        "additionalUserLabels": {}
                    }
                }
            }
    
            url = "https://dataflow.googleapis.com/v1b3/projects/v3-playground-366213/locations/us-central1/flexTemplates:launch"
            
            response = requests.post(url, json=body, headers=headers)
            response_json = response.json()
            job_id = response_json["job"]["id"]
            
            print("JOB_ID - "+job_id)
            
            return job_id
                 
        @task.sensor(poke_interval=10, timeout=2*60*60, mode="reschedule")
        def wait_fortransfer_to_end(job_id) -> PokeReturnValue:
            url = f"https://dataflow.googleapis.com/v1b3/projects/v3-playground-366213/jobs/{job_id}"

            response = requests.get(url, headers=headers)

            # Print the response
            response_json = response.json()
            state = response_json["currentState"]
            return PokeReturnValue(state == "JOB_STATE_DONE" or state == "JOB_STATE_CANCELLED", xcom_value="xcom_value")
         
        @task
        def execute_sql_statement(ret, collectionName):
            task_logger.info(f"Executing insert - {collectionName}")
            client = bigquery.Client()

            PROJECT_ID = "v3-playground-366213"

            # Perform a query.
            QUERY = (f"insert into `{PROJECT_ID}.ETS.Logs`(DateTime,Text) values(current_datetime(),'finised importing - {collectionName}')")
            query_job = client.query(QUERY)
            
            return collectionName

        ret = prepare_destination_table(group_collectionName)
        job_id = start_templated_tranfer(ret, group_collectionName)
        ret = wait_fortransfer_to_end(job_id)
        execute_sql_statement(ret, group_collectionName)
        

    @task
    def GetAllCollections(dependency, part: int, context=None):
        client = bigquery.Client()
        
        numberOfParts = int(dag.params["numberOfParts"])
        dag.log.info(str(dag.params['accountNames']))
        dag.log.info(int(dag.params['numberOfParts']))
        accountsToSync = dag.params['accountNames']
        
        collections = []
        
        namesFilters = ""
        if(len(accountsToSync) > 0):
            for i, account in enumerate(accountsToSync):
                namesFilters += f"'{account}'"
                if(i < len(accountsToSync) - 1):
                    namesFilters += ","
                    
        in_query = ""
        if(namesFilters != ""):
            in_query = "where Name in ("+namesFilters+")"
            
        count_query = ("select count(*) as count from `v3-playground-366213.BI_Master.SystemAccountsRoot` "+in_query)
        
        dag.log.info(count_query)
        count_job = client.query(count_query)
        rows = count_job.result()
        count = 0
        for row in rows:
            count = row["count"]
            break        
        
        dag.log.info(f"Number of rows - {count}")
        
        partDivisor = int(count/numberOfParts)
        
        dag.log.info(f"Part divisor - {partDivisor}")

        QUERY = ("select Id,Name from `v3-playground-366213.BI_Master.SystemAccountsRoot` "+in_query+" order by Id")
        dag.log.info(QUERY)
        query_job = client.query(QUERY)
        rows = query_job.result()
        index = 0
        for row in rows:
            current_part = int(index / partDivisor)
            if(current_part == part):
                collectionName = "Records"+str(row["Id"])
                collections.append(collectionName)
                
                dag.log.info(f"Adding collection {collectionName} int part {part}")
            
            index = index + 1
            
        client.close()
            
        return collections
    
    @task
    def WaitOnCollections(group_collectionName):
        # strCols = ""
        # for i in range(len(collections)):
        #     strCols += collections[i] + ", "

        task_logger.info(f"Just waiting done - {group_collectionName}")
    
        
    numberOfParts = int(dag.params["numberOfParts"])
    dag.log.info(f"Number of parts {numberOfParts}")
    last = None
    
    
    i=0
    
    data_part = GetAllCollections.override(task_id=f"GetCollections_part{i}")(None,i)
    
    one = OneCollectionTransfer.override(group_id=f"Part{i}").expand(group_collectionName=data_part.output),
    wait = WaitOnCollections.partial().expand(group_collectionName=data_part)
    chain(data_part, one, wait)
    last = wait
    
    for i in range(1, numberOfParts):
        data_part = GetAllCollections.override(task_id=f"GetCollections_part{i}")(last, i)
        one = OneCollectionTransfer.override(group_id=f"Part{i}").expand(group_collectionName=data_part),
        wait = WaitOnCollections.partial().expand(group_collectionName=data_part),
        chain(data_part, one, wait)
        last = wait
    
    
    
    # @task
    # def PrintCollectionNames(collection):
    #     for i in range(len(collection)):
    #         task_logger.info(f"PRINT - Index: {i}, Value: {collection[i]}")
    #         print(f"PRINT - Index: {i}, Value: {collection[i]}")
            

        
    # ret = GetAllCollections
    # ret2 = ret.partial().override().expand()
    
    
    # tasks = []
    
    # i=0
    # end = 3
    # while(i < len(tables)):
    #     localList = []
    #     localGroups = []
    #     while(i < end):
    #         if(i >= len(tables)):
    #             break
            
    #         oneTransfer = OneCollectionTransfer.override(group_id=tables[i])(group_collectionName=tables[i])
    #         localList.append(tables[i])
    #         localGroups.append(oneTransfer)
            
    #         i = i + 1
            
    #     tasks.append(PrintCollectionNames(localList))
    #     tasks.append(localGroups)
    #     tasks.append(WaitOnCollections(localList))
        
    #     if(i >= len(tables)):
    #         break
        
    #     i = end
    #     end = end + 3
    
    # chain(*tasks)
