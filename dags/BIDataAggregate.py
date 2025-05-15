import dataclasses
import os
from pathlib import Path
import datetime
import string
from enum import Enum

from dataclasses import dataclass

from typing import Any, Callable, ClassVar, Dict, List
import uuid

from google.cloud import bigquery

import requests
from typing import NamedTuple

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

from BILibrary import Element, ElementType, Account
from BILibrary.bigquery_utils import create_table_from_schema, get_all_materialized_views_ddl

PROJECT_ID = "v3-playground-366213"
DATASET_ORIGIN = "ETS"
DATASET_MASTER = "BI_TemplateModel"
CREATE_OR_REPLACE_VIEW_TEMPLATE = "create or replace view ${dataset}.${viewName} as (${viewDefinition})"

schema_serviceitem = [
    bigquery.SchemaField("Id", "INTEGER"),
    bigquery.SchemaField("SystemAccountRootId", "INTEGER"),
    bigquery.SchemaField("LastChanged", "DateTime"),
    bigquery.SchemaField("Created", "DateTime"),
    bigquery.SchemaField("Data", "JSON")                         
]

schema_history = [
    bigquery.SchemaField("Id", "INTEGER"),
    bigquery.SchemaField("source_data", "JSON"),
    bigquery.SchemaField("timestamp", "TIMESTAMP"),
    bigquery.SchemaField("Created", "DateTime"),
]

def MakeQueryForThisDataSet(datasetname: str, query: str) -> str:
    new_query = new_query.replace(DATASET_MASTER, datasetname)
    new_query = new_query.replace("partition_expiration_days=1000.0","")
    
    return new_query

def MakeCreateOrReplace(query: str) -> str:
    str = query.replace('create function','create or replace function')
    str = str.replace('create procedure', 'create or replace procedure')
    str = str.replace(f'`{PROJECT_ID}`.','')
    
    return str

dag_args = {
    "start_date": datetime.datetime(2025, 5, 8)
}

def IsNotFound(error_result):
    if("message" in error_result and str(error_result['message']).startswith('Not found')):
        return True
    else:
        return False

LOCATION = "us"

data_dir = os.path.dirname(os.path.abspath(__file__)).replace('dags','data')

def GetAllCollections() -> List[Account]:
    file = open(os.path.join(data_dir,dag.params["tables_path"]))
    decoded = json.loads(file.read())
    accounts: List[Account] = []
    
    for account in decoded:
        accounts.append(Account(account['AccountName'],f"Records{account['Id']}", account['Id'], str(account["Id"])+"_"+account['AccountName']))
        
    accounts.sort(key=lambda a: a.AccountName)
    return accounts[0:0+1]

def BreakCollection(list, chunckSize):
    for i in range(0, len(list), chunckSize):
        yield list[i:i + chunckSize]    

with models.DAG(
    "BIDataAggregate",
    description="Match all customers databases with the schema on BI_TemplateModel, mirror views and transform data",
    schedule_interval=None,
    default_args=dag_args,
    params= {
        "tables_path": "all_collections.json",
        "chunk_size": 10
    }
) as dag:
    @task
    def GetViewsAndRoutines(dataset) -> List[Element]:
        client = bigquery.Client()
        elements: List[Element] = []
        
        #Get the views
        query_res = client.query(f"select table_name,view_definition from {dataset}.INFORMATION_SCHEMA.VIEWS")
        rows = query_res.result()
        for row in rows:
            definition = MakeCreateOrReplace(row["view_definition"])
            elements.append(Element(row["table_name"],ElementType.View,definition,definition))
        
        #Get the materialized views
        for view in list(get_all_materialized_views_ddl(client, dataset)):
            elements.append(Element(
                Name = view.name,
                DDL = view.ddl,
                Definition=view.ddl,
                Type=ElementType.MaterializedView
            ))

        #Get the routines (UDFs and procedures)
        query_res = client.query(f"select specific_name,routine_type,routine_definition,ddl from {dataset}.INFORMATION_SCHEMA.ROUTINES")
        rows = query_res.result()
        for row in rows:
            type = ElementType.UDF
            if(row["routine_type"] == "PROCEDURE"):
                type = ElementType.Procedure

            elements.append(Element(row["specific_name"],
                                    type, MakeCreateOrReplace(row["routine_definition"]),
                                    MakeCreateOrReplace(str(row["ddl"]))))
        client.close()
        
        return elements
    
    @task_group()
    def batch_accounts(accounts,elements: List[Element]):
        
        @task_group()
        def account_process(account: Account, elements: List[Element]):
            account_datasetName = "BI_"+account.AccountName
            
            def transformListToDic(elements):
                dic = {}
                for e in elements:
                    dic[e.Name] = e
                
                return dic
            
            def ExecuteQuery(bgClient: bigquery.Client, query: str) -> str:
                qj = bgClient.query(query)
                if(qj.error_result is not None):      
                    dag.log.info("Query error: "+query)
                    if(not IsNotFound(qj.error_result)):
                        dag.log.info(f"Error in query: {query}")
                        raise Exception(f"Error executing query - {str(qj.error_result)}")
                    else:
                        return "TryAgain"
                return "OK"
            
            def CreateOrReplaceElement(element: Element):
                ret = "OK"
                bgClient = bigquery.Client(PROJECT_ID)
                transformed_definition = MakeQueryForThisDataSet(account_datasetName, element.DDL)
                if(element.Type == ElementType.View):
                    ddl_view_template = string.Template(CREATE_OR_REPLACE_VIEW_TEMPLATE)
                    ddl_view = ddl_view_template.substitute(dataset=account_datasetName,viewName=element.Name,viewDefinition=transformed_definition)
                    
                    ret = ExecuteQuery(bgClient, ddl_view)
                elif(element.Type == ElementType.MaterializedView):
                    ret = ExecuteQuery(bgClient, transformed_definition)
                else: #Procedure or Function
                    ret = ExecuteQuery(bgClient, transformed_definition)

                return ret
            
            @task()
            def Setup_Account(account):
                bgClient = bigquery.Client(PROJECT_ID)
                datasets = bgClient.list_datasets(PROJECT_ID)
                found = False
                for ds in datasets:
                    if(str(ds.dataset_id).lower() == account_datasetName.lower()):
                        found = True
                        break
                    
                if(not found): #new account
                    dag.log.info(f"Account {account.AccountName} is new, creating...")
                    new_ds = bgClient.create_dataset(account_datasetName)
                    
                tableId_raw = f"ServiceItemRaw"
                tableId_events= f"ServiceItemEvents"
                tableId_history = f"RecordsAllHistoryP"
                
                table_serviceitemraw = create_table_from_schema(bgClient,PROJECT_ID, DATASET_MASTER, account_datasetName, tableId_raw, schema_serviceitem)
                bgClient.create_table(table_serviceitemraw,exists_ok=True)

                table_serviceitemsevents = create_table_from_schema(bgClient, PROJECT_ID,DATASET_MASTER, account_datasetName,tableId_events, schema_serviceitem)
                bgClient.create_table(table_serviceitemraw,exists_ok=True)

            @task()
            def Sync_MetaData(elements: List[Element], elements_from_account: List[Element]):
                dag.log.info("Elements list: "+str(elements))
                    
                dag.log.info(f'Elements view from account: {str(elements_from_account)}')
                dic_elements_from_account = transformListToDic(elements_from_account)
                
                retries:Dict[str, Element] = {}
                
                for e in elements:
                    ret = ""
                    if e.Name in dic_elements_from_account:
                        e_account = dic_elements_from_account[e.Name]
                        if(e.DDL != e_account.DDL):
                            ret = CreateOrReplaceElement(e)
                            dag.log.info(f'Ret from CreateOrReplace: {e.Name} = {ret}')
                            if(ret == "TryAgain"):
                                retries[e.Name] = e
                    else:
                        ret = CreateOrReplaceElement(e)
                        dag.log.info(f'Ret from CreateOrReplace: {e.Name} = {ret}')
                        if(ret == "TryAgain"):
                            retries[e.Name] = e
                                                
                    dag.log.info(f'Processando elemento {e.Name} da conta {account.AccountName}, kept for second try: {ret}')
                
                i=0
                while(i < 3 or len(retries) == 0):
                    topop = []
                    for key in retries.keys():
                        element = retries[key]
                        dag.log.info(f"Retring {element.Name}, try {i+1}")
                        ret = CreateOrReplaceElement(element)
                        if(ret == "OK"):
                            topop.append(key)
                            
                    for popkey in topop:
                        retries.pop(popkey)
                              
                    i=i+1
                
                                
            @task()
            def Execute_Aggregations(elements: list[Element]):
                # Filtrar somente as views e as views que devem gerar tabela
                for v in elements:
                    dag.log.info(f'Executando elemento {v.Name} da conta {account.AccountName}')
                
            setup = Setup_Account(account)
            elements_from_account = GetViewsAndRoutines(dataset=account_datasetName)
            sinc = Sync_MetaData(elements, elements_from_account)
            execute = Execute_Aggregations(elements)
            
            setup >> elements_from_account >> sinc >> execute
            
        for a in accounts:
            account_process.override(group_id=f'{a.AccountName}-{a.Id}')(a, elements)
        
    all_accounts = GetAllCollections()
    account_broken = list(BreakCollection(all_accounts, dag.params["chunk_size"]))
    
    tasks = []
    getViews = GetViewsAndRoutines(DATASET_MASTER)
    last = None
    for i, account_batch in enumerate(account_broken):
        process = batch_accounts.override(group_id=f"batch{i}")(accounts=account_batch,elements=getViews)
        if(last):
            tasks.append(last >> process)
        else:
            tasks.append(process)
        last = process
        
    chain(tasks)
        
    
    
        
        
        
    