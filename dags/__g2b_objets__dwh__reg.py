from airflow import DAG
import datetime
from airflow.models import Variable
import g2b_objects
import csv_to_s3

dag_name = "g2b_objects__dwh__reg"

with DAG(dag_name,
        schedule_interval='@hourly',
        start_date=datetime.datetime(2021, 11, 7),
        catchup=False,
        tags=['test', 'g2b'],
) as dag:

    from airflow.operators.dummy_operator import DummyOperator
    from airflow.operators.python_operator import PythonOperator
    
    start_step = DummyOperator(task_id="start_step",dag=dag)
    end_step = DummyOperator(task_id="end_step",dag=dag)

    """
    load_json_step_objects = PythonOperator(task_id='load_json_step_objects',
                                            python_callable=load_json.variable_set,
                                            dag=dag
                                             )
    """

    transfomr_file_step_objects = PythonOperator(task_id='transfomr_file_step_objects',
                                                 python_callable=g2b_objects.file_step_g2b_object,
                                                 dag=dag
                                                 
                                                 )
    
    
    transport_file_step_objects = PythonOperator(task_id='transport_file_step_objects',
                                                 python_callabe=csv_to_s3.csv_to_s3,
                                                 dag=dag 
                                                  )   
    


start_step >> transfomr_file_step_objects >> transport_file_step_objects >> end_step