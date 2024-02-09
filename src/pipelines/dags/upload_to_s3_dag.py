import sys
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from parsing.parse import Parser
from scripts.upload_to_s3 import upload_to_s3

default_dag_args={'owner': 'alexchikov',
                  'depends_on_past': False,
                  'start_date': datetime(2024, 2, 8, 8, 30, 0),
                  'email': 'alex.chikov@outlook.com',
                  'email_on_retry': False,
                  'email_on_failure': True,
                  'retries': 5,
                  'retry_delay': timedelta(minutes=5)}

dag = DAG(dag_id='cian_upload_to_s3_dag',
          default_args=default_dag_args,
          description='This DAG uploads parsed JSON from CIAN to S3 bucket',
          schedule_interval=timedelta(hours=12),
          tags=['Cian'])
    
p = Parser()
    
exctract_data = PythonOperator(task_id='extract_date',
                               python_callable=p.get_offers,
                               dag=dag)

load_data = PythonOperator(task_id='load_data',
                           python_callable=upload_to_s3,
                           dag=dag)

exctract_data >> load_data