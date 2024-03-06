import sys
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from parsing.parse import Parser
from parsing.transform_data_to_psql import parse_json_data, insert_data_to_db
from scripts.upload_to_s3 import upload_to_s3

default_dag_args={'owner': 'alexchikov',
                  'depends_on_past': False,
                  'start_date': datetime(2024, 2, 8, 8, 30, 0),
                  'email': 'alex.chikov@outlook.com',
                  'email_on_retry': False,
                  'email_on_failure': True,
                  'retries': 5,
                  'retry_delay': timedelta(minutes=3)}

dag = DAG(dag_id='cian_upload_to_s3_dag',
          default_args=default_dag_args,
          description='This DAG uploads parsed JSON from CIAN to S3 bucket',
          schedule_interval=timedelta(hours=12),
          tags=['Cian'],
          catchup=False)
    
p = Parser()
    
exctract_data = PythonOperator(task_id='extract_date',
                               python_callable=p.get_offers,
                               dag=dag)

load_to_s3 = PythonOperator(task_id='to_s3',
                            python_callable=upload_to_s3,
                            dag=dag)

load_to_db = PythonOperator(task_id='load_to_db',
                            python_callable=insert_data_to_db,
                            dag=dag)

exctract_data.set_downstream([load_to_s3, load_to_db])