from airflow import DAG
from airflow.operators import LoadDimensionOperator
from helpers import SqlQueries

def get_load_dimension_table_dag(
        parent_dag_name,
        task_id,
        *args, **kwargs):
    
    dag = DAG(
        f"{parent_dag_name}.{task_id}",
        **kwargs
    )
    
    return dag    