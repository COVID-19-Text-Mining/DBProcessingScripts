import requests
import json
import urllib
import glob
from pprint import pprint
import re
import pymongo
db_host = "mongodb05.nersc.gov"
db =  "COVID-19-text-mining"
password = ""
user = ""
client = pymongo.MongoClient(db_host, username=user,
                             password=password, authSource=db)
db = client[db]
def clean_title(title):
    clean_title = title.split("Running Title")[0]
    clean_title = re.sub('( [0-9])*$', '', clean_title)
    clean_title = clean_title.replace("Running Title: ", "")
    clean_title = clean_title.replace("Short Title: ", "")
    clean_title = clean_title.replace("Title: ", "")
    clean_title = clean_title.strip()
    return clean_title
for col in db.list_collections():
    print(col['name'])
    for doc in db[col['name']].find({ "tried_crossref_doi" : { "$exists" : False }, "doi" : { "$exists" : False }}):
        metadata = doc['metadata']
        try:
            title = clean_title(metadata['title'])
        except KeyError:
            title = None
        try:
            author_names = ",".join([a['last'] for a in metadata['authors']])
        except KeyError:
            author_names = None
        query_string = 'https://api.crossref.org/works'
        if title:
            query_string = query_string + "?query.bibliographic={}".format(urllib.parse.quote_plus(title))
        if author_names:
            query_string = query_string + "?query.author={}".format(urllib.parse.quote_plus(author_names))
        r = requests.get(query_string)
        try:
            if not any([i['title'][0] == title for i in r.json()['message']['items']]):
                title = re.sub(' [0-9] ', ' ', title)
            print(title)
            for i in r.json()['message']['items']:
                if i['title'][0] == title:
                    print("FOUND")
                    db[col['name']].find_one_and_update({"_id": doc['_id']},
                                     {"$set": {"doi": i['DOI']}})
                    break
        except:
            pass
        db[col['name']].find_one_and_update({"_id": doc['_id']},
             {"$set": {"tried_crossref_doi": True}})
