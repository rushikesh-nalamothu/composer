
import json
import datetime

from airflow import DAG
from airflow import models
from airflow.utils import trigger_rule
import logging
import traceback
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.operators import bigquery_operator
from airflow.providers.google.cloud.operators.bigquery import BigQueryDeleteTableOperator
from airflow.providers.google.cloud.transfers.bigquery_to_gcs import BigQueryToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyTableOperator
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator

from airflow.models import Variable
default_args = {
    'start_date': datetime.datetime(2021, 6, 28),
    'owner': 'rushikesh',
    'depends_on_past': False,
    'email': ['rushikesh.nallamothu@vitap.ac.in'],
    'email_on_failure': False,
    'email_on_retry': False,
    'catchup':False,
     }

DATASET_NAME=Variable.get('dataset')
TEMP_TABLE=Variable.get('temp_table')
FINAL_TABLE=Variable.get('test_table')
PROJECT_ID='training-317008'
dag=DAG('composer_v1',
        default_args=default_args,
        schedule_interval='@daily',)


load_to_bigquery= GCSToBigQueryOperator(
    task_id='load_to_bigquery',
    bucket='composer-task',
    source_objects=['assignment.csv'],
    destination_project_dataset_table=f"{DATASET_NAME}.{TEMP_TABLE}",
    schema_fields=[
        {'name': 'invoice_and_item_number', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'date', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'store_number', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'store_name', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'address', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'city', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'zip_code', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'store_location', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'county_number', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'county', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'category', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'category_name', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'vendor_number', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'vendor_name', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'item_number', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'item_description', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'pack', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'bottle_volume_ml', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'state_bottle_cost', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'state_bottle_retail', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'bottles_sold', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'sale_dollars', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'volume_sold_liters', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'volume_sold_gallons', 'type': 'STRING', 'mode': 'NULLABLE'},
        ],
    skip_leading_rows=1,
    write_disposition='WRITE_TRUNCATE',
    dag=dag,
    )

sql_transform=bigquery_operator.BigQueryOperator(
        dag=dag,
        task_id='sql_transform',
        sql=f"SELECT store_number,sale_dollars FROM {DATASET_NAME}.{TEMP_TABLE}",
        use_leagcy_sql=False,
        destination_dataset_table='airflow.test_table',
        write_disposition='WRITE_TRUNCATE',
        
        )

create_table = BigQueryCreateEmptyTableOperator(
    dag=dag,
    task_id="create_table",
    dataset_id=DATASET_NAME,
    table_id=FINAL_TABLE,
    schema_fields=[
        {"name": "store_name", "type": "STRING", "mode": "NULLABLE"},
        {"name": "sale_dollars", "type": "STRING", "mode": "NULLABLE"},
    ]
)

delete_table = BigQueryDeleteTableOperator(
    dag=dag,
    task_id="delete_table",
    deletion_dataset_table=f"{PROJECT_ID}.{DATASET_NAME}.{TEMP_TABLE}",
)

export_csv__to_gcs = BigQueryToGCSOperator(
    dag=dag,
    task_id='export_csv__to_gcs',
    source_project_dataset_table='airflow.test_table',
    destination_cloud_storage_uris='gs://composer-archive-1/results.csv',
    export_format='CSV')

archive_file= GCSToGCSOperator(
    dag=dag,
    task_id='archive_file',
    source_bucket='composer-task',
    source_objects=['assignment.csv'],
    destination_bucket='composer-archive-1',
    destination_object='assignment.csv',

)


load_to_bigquery >> create_table >> sql_transform >> [delete_table,archive_file,export_csv__to_gcs]
