from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from scripts.upload_all_to_s3 import upload_all_missing_files_to_s3, remove_all_local_files

default_dag_args = {'owner': 'alexchikov',
              'depends_on_past': False,
              'catchup': False,
              'start_date': datetime(2024, 2, 8, 19, 35, 0),
              'email_on_failure': False,
              'email_on_retry': False,
              'retries': 5,
              'retry_delay': timedelta(minutes=5)}

dag = DAG(dag_id='upload_all_missing_to_s3',
          default_args=default_dag_args,
          description='This DAG finds all missing files and upload them to S3. Then remove them.',
          schedule=timedelta(days=1),
          tags=['Cian'])

upload_all_missing = PythonOperator(task_id='upload_all_missing_to_s3',
                                    python_callable=upload_all_missing_files_to_s3,
                                    dag=dag)

remove_all_local = PythonOperator(task_id='remove_all_local_files',
                                        python_callable=remove_all_local_files,
                                        dag=dag)

upload_all_missing >> remove_all_local