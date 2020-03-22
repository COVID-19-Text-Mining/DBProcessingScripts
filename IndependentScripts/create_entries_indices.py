import pymongo
import os

client = pymongo.MongoClient(os.getenv("COVID_HOST"), username=os.getenv("COVID_USER"),
                             password=os.getenv("COVID_PASS"), authSource=os.getenv("COVID_DB"))
db = client[os.getenv("COVID_DB")]
collection = db["entries"]

collection.create_index([("Doi", pymongo.DESCENDING)], unique=True)
collection.create_index([("Title", pymongo.TEXT), ("Abstract", pymongo.TEXT)], default_language ="english")
