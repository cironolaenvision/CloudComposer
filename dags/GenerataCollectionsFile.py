import datetime
import os

from google.cloud import bigquery

# import the logging module
import logging

# get the airflow.task logger
task_logger = logging.getLogger("airflow.task")

from airflow import models

from airflow.decorators import(task) #foi

import json

PROJECT_ID = "v3-playground-366213"
DATASET = "ETS"

dag_args = {
    "start_date": datetime.datetime(2025, 5, 8),
}

LOCATION = "us"

data_dir = os.path.dirname(os.path.abspath(__file__)).replace('dags','data')

with models.DAG(
    dag_id="generate_collections_to_file",
    description="Consulta a tabela de SystemAccounts do BigQuery a salva em um arquivo para outras Dags utilizarem",
    schedule_interval=None
) as dag:

    @task(task_id=f"prepTable")
    def get_records():
        client = bigquery.Client()
        records = []
        
        # Perform a query.
        QUERY = ("select Id,Name from `v3-playground-366213.BI_Master.SystemAccountsRoot` order by Id")
        query_job = client.query(QUERY)
        rows = query_job.result()
        for row in rows:
            records.append({
                "AccountName": row["Name"],
                "Id": row["Id"] 
            })
            
        client.close()
            
        return records
    
    @task()
    def save_to_file(records):
        jsonRecords = json.dumps(records)
        
        dag.log.info(f"writing file to {os.path.join(data_dir, 'all_collections.json')}")
        
        file = open(os.path.join(data_dir, "all_collections.json"), "w")
        file.write(jsonRecords)
        file.close()
        
    records = get_records()
    save_to_file(records)
