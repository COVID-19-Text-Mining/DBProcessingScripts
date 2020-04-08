import os
import pymongo
import sys
import json
import datetime
import re
from tqdm import tqdm
from pprint import pprint
import json

client = pymongo.MongoClient(os.getenv("COVID_HOST"), username=os.getenv("COVID_USER"),
                             password=os.getenv("COVID_PASS"), authSource=os.getenv("COVID_DB"))
db = client[os.getenv("COVID_DB")]

use_test_db = False

#Rebuild the entire entries collection
rebuild = False
#Keys that are allowed in the entries database to keep it clean and minimal
entries_keys = [
    'title',
    'authors',
    'doi',
    'journal',
    'publication_date',
    'abstract',
    'link',
    'category_human',
    'category_ml',
    'keywords',
    'keywords_ml',
    'summary_human',
    'summary_ml',
    'is_covid19',
    'is_covid19_ml',
    'is_covid19_ml_bool',
    'has_year',
    'has_day',
    'has_month',
    'similar_abstracts',
    'is_covid19_ml_bool',
    ]

def strip_down_entry(entry):
    #Turn a document from the entries collection into one suitable for the swiftype API
    #If not possible, return None
    entry_searchable = dict()

    for possibly_list_field in ['title', 'doi', 'journal', 'abstract', 'link', 'is_covid19', 'is_covid19_ml', 'has_year', 'has_month', 'has_day', 'is_covid19_ml_bool']:
        if possibly_list_field in entry.keys():
            if isinstance(entry[possibly_list_field], list):
                stringified = " ".join(entry[possibly_list_field])
            else:
                stringified = entry[possibly_list_field]
            entry_searchable[possibly_list_field] = stringified

    if not 'is_covid19_ml' in entry.keys():
        entry['is_covid19_ml'] = 0.0
    elif not isinstance(entry['is_covid19_ml'], float):
        try:
            entry['is_covid19_ml'] = float(entry['is_covid19_ml'])
        except:
            print("entry", str(entry["_id"]),'has a non-numeric value (', entry['is_covid19_ml'], ') for is_covid_19_mli!')
            if entry['is_covid19_ml'] == False:
                entry['is_covid19_ml'] = 0.0
            elif entry['is_covid19_ml'] == True:
                entry['is_covid19_ml'] = 1.0
            print(f"using default value of 0.0 for is_covid19_ml of entry", str(entry['_id']))
            entry['is_covid19_ml'] = 0.0
    
    entry['is_covid19_ml_bool'] = entry['is_covid19_ml'] > 0.5

    for multiple_opinions_field in ['category_human', 'category_ML', 'summary_human', 'summary_ML']:
        if isinstance(entry.get(multiple_opinions_field, ""), list):
            if len(entry[multiple_opinions_field]) > 0:
                stringified = entry[multiple_opinions_field][0]
            else:
                stringified = ""
        else:
            stringified = entry.get(multiple_opinions_field, "")
        entry_searchable[multiple_opinions_field.lower()] = stringified

    for definitely_list_field in ['keywords', 'keywords_ML']:
        value = entry.get(definitely_list_field, [])
        if value is None:
            stringified = ""
        elif isinstance(value,list):
            stringified = ", ".join(value)
        else:
            stringified = ""  
        entry_searchable[definitely_list_field.lower()] = stringified  

    authors_obj = entry.get('authors', [])
    if authors_obj is None:
        authors_obj = []
    full_author_list = []
    for author in authors_obj:
        if "name" in author:
            full_author_list.append(author["name"])
        elif "Name" in author:
            full_author_list.append(author["Name"])
    authors = ", ".join(full_author_list)
    entry_searchable['authors'] = authors

    entry_searchable['is_covid19'] = str(entry['is_covid19'])

    if entry['publication_date'] is not None:
        if entry['publication_date'] > datetime.datetime.now() and '_bt' in entry.keys():
            pub_date = entry['_bt']
        else:
            pub_date = entry['publication_date']
        entry_searchable['publication_date'] = pub_date.isoformat()+"Z"
    else:
        entry_searchable['publication_date'] = datetime.datetime(year=1,month=1,day=1).isoformat()+"Z"
        entry_searchable['has_year'] = False
        entry_searchable['has_month'] = False
        entry_searchable['has_day'] = False


    try:
        json.dumps(entry_searchable)
        return entry_searchable
    except TypeError:
        return None


for doc in tqdm(list(db.entries.find())):
    if use_test_db:
        collection_name = "entries_searchable_test"
    else: 
        collection_name = "entries_searchable"
    stripped_down = strip_down_entry(doc)
    if stripped_down:
        existing_entry = db[collection_name].find_one({"doi": doc["doi"]})
        if existing_entry:
            db[collection_name].find_one_and_replace({"doi": doc["doi"]}, stripped_down)
        else:
            db[collection_name].insert_one(stripped_down)
