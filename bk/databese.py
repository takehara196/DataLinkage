from pymongo import MongoClient,  DESCENDING
import os, sys, configparser

PYMONGO_DIR = os.path.abspath(os.path.dirname(os.path.abspath(__file__)))


def get_db(db_name):
    config = configparser.ConfigParser()
    config.read(f"{PYMONGO_DIR}/config.ini")
    client = MongoClient('localhost')
    client['admin'].authenticate(config.get('mongo', 'id'), config.get('mongo', 'password'))
    db = client[db_name]
    return db

def load_colection():
    df = get_db(zips)


    return