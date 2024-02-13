from parse import Parser
from dags.config import DAGConfig as cfg
from pyspark.sql import SparkSession, functions as F
import yaml
import json

with open(f'{cfg.CONFIGS_PATH}/s3.yml') as file:
    config_file = yaml.safe_load(file)
    SECRET_ACCESS_KEY = config_file['secret_access_key']
    MY_KEY = config_file['my_key']
    BUCKET = config_file['bucketname']
    
def parse_json_data(file: str):
    with open('/home/aleks/Документы/cian_parsing_and_analyse/files/cian_2024-02-12_23:02:20.json') as json_file:
        json_file_data = json.load(json_file)["data"]["offersSerialized"]

    spark = SparkSession().builder.master('local[1]').appName('Cian parsing').getOrCreate()
    
    offerId = json_file_data["id"]
    isApartments = json_file_data["isApartments"]
    addedTimestamp = json_file_data["addedTimestamp"]
    fullUrl = json_file_data["fullUrl"]
    underground_name = json_file_data["geo"]["undergrounds"][0]["name"]
    underground_transportType = json_file_data["geo"]["undergrounds"][0]["transportType"]
    underground_time = json_file_data["geo"]["undergrounds"][0]["time"]
    flatType = json_file_data["flatType"]
    description = json_file_data["description"]
    floorNumber = json_file_data["floorNumber"]
    price = json_file_data["bargainTerms"]["price"]
    deposit = json_file_data["bargainTerms"]["deposit"]
    leaseTermType = json_file_data["bargainTerms"]["leaseTermType"]
    paymentPeriod = json_file_data["bargainTerms"]["paymentPeriod"]
    floorsCount = json_file_data["building"]["floorsCount"]
    buildYear = json_file_data["building"]["buildYear"]
    photo1 = json_file_data["photos"][0]["fullUrl"]
    photo2 = json_file_data["photos"][1]["fullUrl"]
    photo3 = json_file_data["photos"][2]["fullUrl"]
    phone = json_file_data["phones"]["countryCode"] + json_file_data["phones"]["number"]

    df = spark.createDataFrame({"offerId": offerId,
                                "isApartments": isApartments,
                                "addedTimestamp": addedTimestamp,
                                "fullUrl": fullUrl,
                                "undergroundName": underground_name,
                                "undergroundTransportType": underground_transportType,
                                "undergroundTime": underground_time,
                                "flatType": flatType,
                                "description": description,
                                "floorNumber": floorNumber,
                                "price": price})
    df.show()

parse_json_data('cian_2024-02-12_23:02:20.json')