
#This works of course only in my airflow container

from airflow import DAG
from airflow.decorators import task
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.hooks.base import BaseHook
from airflow.operators.python import PythonOperator
from datetime import datetime
from google.cloud import bigquery
import json
import pandas as pd
from google.oauth2 import service_account


source_project_id = "bigquery-public-data"
source_dataset_id = "usa_names"
source_table = "usa_1910_current"

dest_project_id = "atlantean-stock-385411"
dest_dataset_id = "usa_names"
dest_table = "names"



with DAG(
    dag_id = 'exc_17_dag',
    start_date = datetime(2025,12,31),
    schedule = None,
    catchup = False
) as dag:
    
    read_query = f"""
        SELECT *
        FROM `bigquery-public-data.usa_names.usa_1910_current` 
        ORDER BY RAND()
        LIMIT 1000
    """


    extract_task = BigQueryInsertJobOperator(
        task_id="extract_task",
        gcp_conn_id="sql_conn",
        configuration={
            "query": {
                "query": read_query,
                "useLegacySql": False,
                "destinationTable": {
                    "projectId": dest_project_id,
                    "datasetId": dest_dataset_id,
                    "tableId": "temp_extract_table",
                },
                "writeDisposition": "WRITE_TRUNCATE",
            }
        },
    )

    def transformer():
        conn = BaseHook.get_connection("sql_conn")
        keyfile_dict = json.loads(conn.extra_dejson.get("keyfile_dict"))
        credentials = service_account.Credentials.from_service_account_info(keyfile_dict)    

        client = bigquery.Client(
            project=conn.extra_dejson.get(dest_project_id),
            credentials=credentials
        )

        temp_table = f"{dest_project_id}.{dest_dataset_id}.temp_extract_table"
        df = client.query(f"SELECT * FROM `{temp_table}`").to_dataframe()
        #Some modifications would be here
        df = df[df['state'] == 'IA']

        filepath = "/tmp/transformed.csv"
        df.to_csv(filepath, index = False)
        return filepath

    
    transform_task = PythonOperator(
        task_id = 'transform_task',
        python_callable = transformer
    )

    def load_to_bq(ti):
        #pull filepath from previous task
        filepath = ti.xcom_pull(task_ids='transform_task')
        print("got file: ", filepath)

        conn = BaseHook.get_connection("sql_conn")
        keyfile_dict = json.loads(conn.extra_dejson.get("keyfile_dict"))
        credentials = service_account.Credentials.from_service_account_info(keyfile_dict) 

        client = bigquery.Client(
            project=conn.extra_dejson.get(dest_project_id),
            credentials=credentials  
        )

        job = client.load_table_from_file(
        open(filepath, "rb"),
        f"{dest_project_id}.{dest_dataset_id}.{dest_table}",
        job_config=bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.CSV,
            autodetect=True,
            write_disposition="WRITE_TRUNCATE"

        ),
        )


    upload_task = PythonOperator(
        task_id = 'upload_task',
        python_callable = load_to_bq,
        #provide_context = True
    )


    extract_task >> transform_task >> upload_task

