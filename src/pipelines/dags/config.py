import yaml
from dataclasses import dataclass

with open('/home/aleks/Документы/cian_parsing_and_analyse/src/pipelines/dags/dag.yml') as file:
    config_file = yaml.safe_load(file)
    
@dataclass
class DAGConfig:
    CONFIGS_PATH: str = config_file['S3_CONFIG_PATH']
    FILES_PATH: str = config_file['FILES_PATH']
    LOGS_PATH: str = config_file['LOGS_PATH']