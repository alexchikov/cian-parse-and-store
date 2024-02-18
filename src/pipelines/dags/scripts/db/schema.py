from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.types import Integer, String, Boolean, TIMESTAMP, Text
from sqlalchemy import create_engine, Column
import yaml
import os

here = os.path.dirname(os.path.abspath(__file__))
filename = os.path.join(here, 'db_config.yml')
Base = declarative_base()


class Offers(Base):
    id = Column("offerId",
                Integer(),
                nullable=False,
                primary_key=True)
    is_apartments = Column("isApartments",
                           Boolean())
    added_timestamp = Column("addedTimestamp",
                             TIMESTAMP())
    full_url = Column("fullUrl",
                      String())
    underground_name = Column("undergroundName",
                              String())
    underground_transport_type = Column("undergroundTransportType",
                                        String())
    underground_time = Column("undergroundTime",
                              Integer())
    flatType = Column("flatType",
                      String())
    description = Column("description",
                         Text())
    floor_number = Column("floorNumber",
                          Integer())
    price = Column("price",
                   Integer())
    deposit = Column("deposit",
                     Integer())
    lease_term_type = Column("leaseTermType",
                             String())
    payment_period = Column("paymentPeriod",
                            String())
    floors_count = Column("floorsCount",
                          Integer())
    buildYear = Column("buildYear",
                        Integer())
    photo1 = Column("photo1",
                    String())
    photo2 = Column("photo2",
                    String())
    photo3 = Column("photo3",
                    String())
    phone = Column("phone",
                    String())
    __tablename__ = "Offers"
    

with open(filename) as file:
    config_file = yaml.safe_load(file)
    DB_HOST = config_file['DB_HOST']
    DB_PORT = config_file['DB_PORT']
    DB_USERNAME = config_file['DB_USERNAME']
    DB_NAME = config_file['DB_NAME']
    DB_PASSWORD = config_file['DB_PASSWORD']

engine = create_engine(f"postgresql://{DB_USERNAME}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}")
Base.metadata.create_all(engine)