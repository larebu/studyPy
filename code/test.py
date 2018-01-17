import sqlalchemy as SqlClient
import pymysql
from pymongo import MongoClient
import datetime

client = MongoClient('192.168.8.6', 27017)
mongo_db = client['test']
engine = SqlClient.create_engine("mysql+pymysql://root:root456@192.168.8.6/remoteSmallDB",
                                 encoding='latin1', echo=True)
meta = SqlClient.MetaData(bind=engine, reflect=True)
table = SqlClient.Table('table_test', meta)

with engine.connect() as con:
    for collection_name in mongo_db.collection_names():
        collect = mongo_db[collection_name]
        i = 10
        for info in collect.find():
            if i < 0:
                break
            i -= 1

            sql = table.insert().values(x=info['x'])
            con.execute(sql)
            print(info)

