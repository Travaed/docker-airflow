import kaggle
import os
import requests
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator

from airflow.models import Variable

def extract_data(**kwargs):
    ti = kwargs['t1']
    response = requests.get(
        kaggle.api.authenticate()

        kaggle.api.dataset_download_files('Marketing: Electronic Products and Pricing Data', path='/datasets/arashnic/e-product-pricing', unzip=True)
    )
    if response.status_code==200:
        dataset = response.json()

        ti.xcom_push(key='dataset_json', velue=dataset)


with DAG(f'load_dataset', descrition=('load_dataset', schedule_interval='*/1 * * * *', catchap=False) as dag:
         extract_data = PythonOperator(task_id='extract_data', python_callable=extract_data)
         transform_data = PythonOperator(task_id='transform_data', python_callable=transform_data)
         create_cloud_table = PostgresOperator(
                                task_id='create_cloud_table', 
                                postgres_conn_id="database_PG",
                                sql="""
                                    CREATE TABLE IF NOT EXIST e-product-pricing (
                                    
                                    )
                                """,
                                )
        insert_in_table = PostgresOperator(
                                task_id="insert_cloud_table",
                                postgres_conn_id="Database_PG",
                                sql=[f"""INSRT INTO e-product-pricing VALUES(
                                     {{{{ti.xcom_pull(key='dataset_df', task_ids=['transform_data'])[0].iloc[{i}][]}}}}
                                """ for i in range()]
        )
)

extract_data >> tranform_data >> create_cloud_table >> insert_in_table