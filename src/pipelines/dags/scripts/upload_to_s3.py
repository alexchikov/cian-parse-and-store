import boto3
import os
import yaml
import logging
from config import DAGConfig as cfg


def log_init():
    logging.basicConfig(filename=f'{cfg.LOGS_PATH}/uploader.log',
                        format='[%(asctime)s] %(levelname)s: %(message)s')


with open(f'{cfg.CONFIGS_PATH}/s3.yml') as file:
    s3_config = yaml.safe_load(file)
    BUCKET = s3_config['bucketname']
    SECRET_ACCESS_KEY = s3_config['secret_access_key']
    MY_KEY = s3_config['my_key']


def get_latest_file():
    log_init()
    try:
        ls_dir = os.listdir(cfg.FILES_PATH)
        ls_dir.sort(reverse=True)
        logging.info('Successfully executed latest filename from files dir')
        return ls_dir[0]
    except IndexError:
        logging.warning('No new filenames in files dir')
        return []


def upload_to_s3():
    filename = get_latest_file()
    if filename:
        try:
            s3 = boto3.client('s3', aws_access_key_id=MY_KEY,
                              aws_secret_access_key=SECRET_ACCESS_KEY)
            s3.upload_file(f'{cfg.FILES_PATH}/{filename}', BUCKET, filename)
            logging.info('Successfully uploaded file to S3 bucket')
            return {'message': 'Task completed with no errors',
                    'code': 0}
        except Exception as exc:
            logging.error(f'{exc.__str__()}')
    else:
        return []
