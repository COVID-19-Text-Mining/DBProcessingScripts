import pymongo
import os

client = pymongo.MongoClient(os.getenv("COVID_HOST"), username=os.getenv("COVID_USER"),
                             password=os.getenv("COVID_PASS"), authSource=os.getenv("COVID_DB"))
db = client[os.getenv("COVID_DB")]
collection = db["entries"]

collection.create_index([("doi", pymongo.DESCENDING)], unique=True)
collection.create_index([("title", pymongo.TEXT), ("abstract", pymongo.TEXT), ("authors", pymongo.TEXT), ("journal", pymongo.TEXT), ("keywords", pymongo.TEXT), ("summary_human", pymongo.TEXT)], name="text_search_index", default_language ="english")

collection = db["google_form_submissions"]

collection.create_index([("doi", pymongo.DESCENDING)])
collection.create_index([("title", pymongo.TEXT), ("abstract", pymongo.TEXT), ("authors", pymongo.TEXT), ("journal", pymongo.TEXT), ("keywords", pymongo.TEXT), ("summary_human", pymongo.TEXT)], name="text_search_index", default_language ="english")
