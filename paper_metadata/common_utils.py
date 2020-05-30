import json
from pprint import pprint
import difflib
import Levenshtein
import pymongo
import regex
from datetime import datetime
import requests
import warnings
import os

###########################################
# communicate with mongodb
###########################################

def get_mongo_db(config_file_path):
    """
    read config file and return mongo db object

    :param config_file_path: (str or None)
        If it is str, which is path to the config.json file. config is a dict of dict as
            config = {
                'mongo_db': {
                    'host': 'mongodb05.nersc.gov',
                    'port': 27017,
                    'db_name': 'COVID-19-text-mining',
                    'username': '',
                    'password': '',
                }
            }
    :return: (obj) mongo database object
    """
    if os.path.exists(config_file_path):
        with open(config_file_path, 'r') as fr:
            config = json.load(fr)

        client = pymongo.MongoClient(
            host=config['mongo_db']['host'],
            port=config['mongo_db']['port'],
            username=config['mongo_db']['username'],
            password=config['mongo_db']['password'],
            authSource=config['mongo_db']['db_name'],
        )

        db = client[config['mongo_db']['db_name']]
    else:
        client = pymongo.MongoClient(
            host=os.getenv("COVID_HOST"),
            username=os.getenv("COVID_USER"),
            password=os.getenv("COVID_PASS"),
            authSource=os.getenv("COVID_DB")
        )
        db = client[os.getenv("COVID_DB")]
    return db
