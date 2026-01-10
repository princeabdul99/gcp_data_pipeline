
from airflow.sdk import DAG
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator 
from datetime import datetime



GCP_PROJECT_ID = "ecom-pipeline-gcp"

args = {
    'owner': 'abdul-dev'
}


with DAG(
    dag_id = 'dag_load_bigquery',
    default_args=args,
    schedule='0 5 * * *',
    start_date=datetime(2026, 1, 1),

) as dag:
    gcs_to_bq_stores = GCSToBigQueryOperator(
        task_id = "gcs_to_bq_stores",
        bucket='{}-bucket'.format(GCP_PROJECT_ID),
        gcp_conn_id="google_cloud_default",
        source_objects=['stores/stores.csv'],
        destination_project_dataset_table='bronze.stores',
        schema_fields = [
            {'name': 'store_id', 'type': 'INTEGER', 'mode': 'NULLABLE'},
            {'name': 'country', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'city', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'store_name', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'number_of_employees', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'zip_code', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'latitude', 'type': 'FLOAT64', 'mode': 'NULLABLE'},
            {'name': 'longitude', 'type': 'FLOAT64', 'mode': 'NULLABLE'},
        ],
        write_disposition='WRITE_TRUNCATE'
    )


    gcs_to_bq_stores

if __name__ == "__main__":
    dag.cli()
