from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
import os

sys.path.append('../../pipelines')

from pipelines.plugins.upload_to_s3 import upload_to_s3
from parsing.parse import Parser

default_dag_args={'owner': 'alexchikov',
                  'depends_on_past': False,
                  'start_date': datetime(2024, 2, 8, 11, 30, 0),
                  'email': 'alex.chikov@outlook.com',
                  'email_on_retry': False,
                  'email_on_failure': True,
                  'retries': 5,
                  'retry_delay': timedelta(minutes=5)}

dag = DAG(dag_id='cian_upload_to_s3',
          description='This DAG uploads parsed JSON from CIAN to S3 bucket',
          schedule_interval=timedelta(hours=12),
          default_args=default_dag_args,
          tags=['Cian'])
    
p = Parser()
    
exctract_data = PythonOperator(task_id='extract_date',
                               python_callable=p.get_offers,
                               dag=DAG)

load_data = PythonOperator(task_id='load_data',
                           python_callable=upload_to_s3,
                           dag = DAG)

exctract_data >> load_data