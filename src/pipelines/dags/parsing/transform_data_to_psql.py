from scripts.upload_to_s3 import get_latest_file
from config import DAGConfig as cfg
from sqlalchemy.orm import Session
from scripts.db.schema import Offers, engine
from datetime import datetime
import yaml
import json

# with open(f'{cfg.CONFIGS_PATH}/s3.yml') as file:
#     config_file = yaml.safe_load(file)
#     SECRET_ACCESS_KEY = config_file['secret_access_key']
#     MY_KEY = config_file['my_key']
#     BUCKET = config_file['bucketname']
    
def parse_json_data():
    file = get_latest_file()
    with open(f'{cfg.FILES_PATH}/{file}') as json_file:
        json_file_data = json.load(json_file)["data"]["offersSerialized"]
    
    for offer in json_file_data:
        offerId = offer["id"]
        isApartments = offer["isApartments"]
        addedTimestamp = datetime.fromtimestamp(offer["addedTimestamp"])
        fullUrl = offer["fullUrl"]
        if "undergrounds" in offer["geo"].keys() and offer["geo"]["undergrounds"]:
            # print(offer["geo"]["undergrounds"][0])
            underground_name = offer["geo"]["undergrounds"][0]["name"]
            underground_transportType = offer["geo"]["undergrounds"][0]["transportType"]
            underground_time = offer["geo"]["undergrounds"][0]["time"]
        else:
            underground_name = None
            underground_transportType = None
            underground_time = None
        flatType = offer["flatType"]
        description = offer["description"]
        floorNumber = offer["floorNumber"]
        price = offer["bargainTerms"]["price"]
        deposit = offer["bargainTerms"]["deposit"]
        leaseTermType = offer["bargainTerms"]["leaseTermType"]
        paymentPeriod = offer["bargainTerms"]["paymentPeriod"]
        floorsCount = offer["building"]["floorsCount"]
        buildYear = offer["building"]["buildYear"]
        photo1 = offer["photos"][0]["fullUrl"]
        photo2 = offer["photos"][1]["fullUrl"]
        photo3 = offer["photos"][2]["fullUrl"]
        phone = offer["phones"][0]["countryCode"] + offer["phones"][0]["number"]
        yield (offerId, isApartments, addedTimestamp, fullUrl,
               underground_name, underground_transportType, underground_time, 
               flatType, description, floorNumber, price, deposit,
               leaseTermType, paymentPeriod, floorsCount, buildYear,
               photo1, photo2, photo3, phone)
        
def insert_data_to_db():
    file = get_latest_file()
    session = Session(bind=engine)
    ids = list(map(lambda x: x[0], session.query(Offers.id).all()))
    offers = list(parse_json_data())
    for offer in offers:
        if offer[0] not in ids:
            new_offer = Offers(id=offer[0],
                               is_apartments=offer[1],
                               added_timestamp=offer[2],
                               full_url=offer[3],
                               underground_name=offer[4],
                               underground_transport_type=offer[5],
                               underground_time=offer[6],
                               flatType=offer[7],
                               description=offer[8],
                               floor_number=offer[9],
                               price=offer[10],
                               deposit=offer[11],
                               lease_term_type=offer[12],
                               payment_period=offer[13],
                               floors_count=offer[14],
                               buildYear=offer[15],
                               photo1=offer[16],
                               photo2=offer[17],
                               photo3=offer[18],
                               phone=offer[19])
            session.add(new_offer)
    session.commit()
    session.close()