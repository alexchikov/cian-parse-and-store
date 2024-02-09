import boto3
import yaml
import os
import logging
from config import DAGConfig as cfg
from .upload_to_s3 import log_init

log_init()

with open(f'{cfg.CONFIGS_PATH}/s3.yml') as file:
    s3_config = yaml.safe_load(file)
    BUCKET = s3_config['bucketname']
    SECRET_ACCESS_KEY = s3_config['secret_access_key']
    MY_KEY = s3_config['my_key']


def upload_all_missing_files_to_s3():
    try:
        s3 = boto3.client('s3', 
                          aws_secret_access_key=SECRET_ACCESS_KEY, 
                          aws_access_key_id=MY_KEY)
        bucket_ls_dir = [data['Key'] for data in s3.list_objects(Bucket=BUCKET)['Contents']]
        local_ls_dir = os.listdir(cfg.FILES_PATH)
        print(bucket_ls_dir)
        for filename in local_ls_dir:
            if filename not in bucket_ls_dir:
                s3.upload_file(
                    f'{cfg.FILES_PATH}/{filename}', BUCKET, filename)
        return {'message': 'Task completed with no errors',
                'code': 0}

    except Exception as exc:
        logging.error(exc.__str__())
        return {'message': exc.__str__(),
                'code': 4}


def remove_all_local_files():
    for filename in os.listdir(cfg.FILES_PATH):
        os.remove(f'{cfg.FILES_PATH}/{filename}')