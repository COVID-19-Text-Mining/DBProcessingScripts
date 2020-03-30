import pymongo
import os

client = pymongo.MongoClient(os.getenv("COVID_HOST"), username=os.getenv("COVID_USER"),
                             password=os.getenv("COVID_PASS"), authSource=os.getenv("COVID_DB"))
db = client[os.getenv("COVID_DB")]
collection = db["entries_trial"]

collection.create_index([("doi", pymongo.DESCENDING)], unique=True)
collection.create_index([("title", pymongo.TEXT), ("abstract", pymongo.TEXT), ("authors", pymongo.TEXT), ("journal", pymongo.TEXT), ("keywords", pymongo.TEXT), ("keywords_ML", pymongo.TEXT), ("summary_human", pymongo.TEXT)], name="text_search_index", default_language ="english")

# collection = db["google_form_submissions"]

# collection.create_index([("doi", pymongo.DESCENDING)])
# collection.create_index([("title", pymongo.TEXT), ("abstract", pymongo.TEXT), ("authors", pymongo.TEXT), ("journal", pymongo.TEXT), ("keywords", pymongo.TEXT), ("summary_human", pymongo.TEXT)], name="text_search_index", default_language ="english")

# origin_collections = [
#   'google_form_submissions',
#   'Scraper_connect_biorxiv_org',
#   'CORD_noncomm_use_subset',
#   'CORD_comm_use_subset',
#   'CORD_biorxiv_medrxiv',
#   'CORD_custom_license',
#   'CORD_metadata']

# parsed_collections = [a + "_parsed" for a in origin_collections]
# for coll in origin_collections + parsed_collections:
# 	# db[coll].create_index([('title', pymongo.DESCENDING)])
# 	db[coll].create_index([('doi', pymongo.DESCENDING)])
# 	db[coll].create_index([('last_updated', pymongo.DESCENDING)])
# 	db[coll].create_index([('_bt', pymongo.DESCENDING)])