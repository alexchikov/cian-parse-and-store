from parsing.parse import Parser
from config import DAGConfig as cfg
import yaml

with open(f'{cfg.CONFIGS_PATH}/s3.yml') as file:
    config_file = yaml.safe_load(file)
    SECRET_ACCESS_KEY = config_file['secret_access_key']
    MY_KEY = config_file['my_key']
    BUCKET = config_file['bucketname']
    
def parse_json_data(file: str):
    ...