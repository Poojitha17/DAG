from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

from airflow.hooks import S3Hook
from datetime import datetime, timedelta
import os


S3_CONN_ID='workshop'
BUCKET='bucket'

name='poojitha' 


def upload_to_s3(file_name):

    # Instanstiaute
    s3_hook=S3Hook(aws_conn_id=S3_CONN_ID) 
    
    # Create file
    sample_file = "{0}_file_{1}.txt".format(name, file_name) 
    example_file = open(sample_file, "w+")
    example_file.write("Putting some data in for task {0}".format(file_name))
    example_file.close()
    
    s3_hook.load_file(sample_file, 'globetelecom/{0}'.format(sample_file), bucket_name=BUCKET, replace=True)

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

with DAG('s3_upload',
         start_date=datetime(2019, 1, 1),
         max_active_runs=1,
         default_args=default_args,
         ) as dag:

    t0 = DummyOperator(task_id='start')

    for i in range(0,5): 
        generate_files=PythonOperator(
            task_id='generate_file_{0}_{1}'.format(name, i),
            python_callable=upload_to_s3,
            op_kwargs= {'file_name': i}
        )

        t0 >> generate_files
