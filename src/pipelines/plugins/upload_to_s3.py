import boto3
import os
import sys
import yaml
import logging

sys.path.insert(0,os.path.abspath(os.path.dirname(__file__)))

logging.basicConfig(filename='logs/uploader.log',
                    level=logging.INFO,
                    format='[%(asctime)s] %(levelname)s: %(message)s')


with open('configs/s3.yml') as file:
    s3_config = yaml.safe_load(file)
    BUCKET = s3_config['bucketname']
    SECRET_ACCESS_KEY = s3_config['secret_access_key']
    MY_KEY = s3_config['my_key']

def get_latest_file():
    try:
        ls_dir = os.listdir('files/')
        ls_dir.sort(key=lambda x: x.split('_')[1])
        logging.info('Successfully executed latest filename from files dir')
        return ls_dir[0]
    except IndexError:
        logging.warning('No new filenames in files dir')
        return []

def upload_to_s3():
    filename = get_latest_file()
    if filename:
        try:
            s3 = boto3.client('s3', aws_access_key_id=MY_KEY, aws_secret_access_key=SECRET_ACCESS_KEY)
            s3.upload_file(f'files/{filename}', BUCKET, filename)
            logging.info('Successfully uploaded file to S3 bucket')
            return 1
        except Exception as exc:
            logging.error(f'{exc.__str__()}')
    else:
        return []