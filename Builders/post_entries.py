import os
import pymongo
import sys
import json
from elastic_app_search import Client

from pprint import pprint
import itertools

def grouper(n, iterable):
    it = iter(iterable)
    while True:
        chunk = tuple(itertools.islice(it, n))
        if not chunk:
            return
        yield chunk

client = pymongo.MongoClient(os.getenv("COVID_HOST"), username=os.getenv("COVID_USER"),
                             password=os.getenv("COVID_PASS"), authSource=os.getenv("COVID_DB"))
db = client[os.getenv("COVID_DB")]


doc_post_url=os.getenv("APPSEARCH_API_ENDPOINT")+"/api/as/v1/engines/entries/documents"

elastic_app_client = Client(
    base_endpoint='{}/api/as/v1'.format(os.getenv("APPSEARCH_API_ENDPOINT")),
    api_key=os.getenv("APPSEARCH_API_KEY"),
    use_https=False
)

for docs in grouper(100, db.entries_searchable.find({"category_ML": {"$exists": False}})):
	for doc in docs:
		doc['id'] = str(doc['_id'])
		del(doc['_id'])
	pprint(elastic_app_client.index_documents("entries", docs))