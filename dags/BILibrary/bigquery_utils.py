"""
BigQuery utility functions for handling materialized views.
"""

from typing import List, Dict, Optional, Union
from dataclasses import dataclass
from google.cloud import bigquery
from google.cloud.bigquery.table import Table, TimePartitioning, RangePartitioning

@dataclass
class MaterializedView:
    name: str
    ddl: str

def create_table_from_schema(
    client: bigquery.Client,
    project: str,
    source_dataset: str,
    destination_dataset: str,
    table_id: str,
    schema: List[bigquery.SchemaField]
) -> Table:
    """
    Create a new table object with schema and partitioning information from an existing table.
    
    Args:
        client: BigQuery client instance
        table_id: Full table ID (project.dataset.table)
        schema: List of SchemaField objects defining the table structure
        
    Returns:
        Table object with schema and partitioning information
    """
    # Get the existing table to copy partitioning info
    existing_table = client.get_table(f"{source_dataset}.{table_id}")
    
    # Create new table object
    table = Table(f"{project}.{destination_dataset}.{table_id}", schema=schema)
    
    # Copy partitioning information if it exists
    if existing_table.time_partitioning:
        table.time_partitioning = TimePartitioning(
            type_=existing_table.time_partitioning.type_,
            field=existing_table.time_partitioning.field,
            expiration_ms=existing_table.time_partitioning.expiration_ms
        )
    elif existing_table.range_partitioning:
        table.range_partitioning = RangePartitioning(
            field=existing_table.range_partitioning.field,
            range_=existing_table.range_partitioning.range_
        )
    
    # Copy clustering information if it exists
    if existing_table.clustering_fields:
        table.clustering_fields = existing_table.clustering_fields
    
    # Copy table description if it exists
    if existing_table.description:
        table.description = existing_table.description
    
    return table
    

def get_materialized_views(client: bigquery.Client, dataset_id: str) -> list[MaterializedView]:
    """
    Get all materialized views in a dataset.
    
    Args:
        client: BigQuery client instance
        dataset_id: Dataset ID to search in
        
    Returns:
        List of materialized view names
    """
    query = f"""
    SELECT table_name,ddl
    FROM `{dataset_id}.INFORMATION_SCHEMA.TABLES`
    WHERE table_type = 'MATERIALIZED VIEW'
    """
    
    query_job = client.query(query)
    results = query_job.result()
    
    views = []
    for row in results:
        views.append(MaterializedView(name=row["table_name"], ddl=row["ddl"]))
        
    return views


def get_all_materialized_views_ddl(client: bigquery.Client, dataset_id: str) -> list[MaterializedView]:
    """
    Get DDL for all materialized views in a dataset.
    
    Args:
        client: BigQuery client instance
        dataset_id: Dataset ID to search in
        
    Returns:
        List of MaterializedView objects containing name and DDL
    """
    views = list(get_materialized_views(client, dataset_id))
    ret = []
    for view in views:
        ret.append(MaterializedView(
            name=view.name,
            ddl=view.ddl
        ))
        
    return ret;
