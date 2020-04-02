import os
import pymongo
import sys
import json
import datetime
import re
from pprint import pprint
import json

client = pymongo.MongoClient(os.getenv("COVID_HOST"), username=os.getenv("COVID_USER"),
                             password=os.getenv("COVID_PASS"), authSource=os.getenv("COVID_DB"))
db = client[os.getenv("COVID_DB")]

#Rebuild the entire entries collection
rebuild = False
#Keys that are allowed in the entries database to keep it clean and minimal
entries_keys = ['title',
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
    'is_covid19'
    ]

def strip_down_entry(entry):
    #Turn a document from the entries collection into one suitable for the swiftype API
    #If not possible, return None
    entry_searchable = dict()

    for possibly_list_field in ['title', 'doi', 'journal', 'abstract', 'link', 'is_covid19']:
        if isinstance(entry[possibly_list_field], list):
            stringified = " ".join(entry[possibly_list_field])
        else:
            stringified = entry[possibly_list_field]
        entry_searchable[possibly_list_field] = stringified

    for multiple_opinions_field in ['category_human', 'category_ML', 'summary_human', 'summary_ML']:
        if isinstance(entry[multiple_opinions_field], list):
            if len(entry[multiple_opinions_field]) > 0:
                stringified = entry[multiple_opinions_field][0]
            else:
                stringified = ""
        else:
            stringified = entry[multiple_opinions_field]
        entry_searchable[multiple_opinions_field.lower()] = stringified

    for definitely_list_field in ['keywords', 'keywords_ML']:
        if entry[definitely_list_field] is None:
            stringified = ""
        else:
            stringified = ", ".join(entry[definitely_list_field])  
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
        entry_searchable['publication_date'] = entry['publication_date'].isoformat()
    else:
        entry_searchable['publication_date'] = ""

    try:
        json.dumps(entry_searchable)
        return entry_searchable
    except TypeError:
        return None


for doc in db.entries.find():

    stripped_down = strip_down_entry(doc)
    if stripped_down:
        existing_entry = db.entries_searchable.find_one({"doi": doc["doi"]})
        if existing_entry:
            db.entries_searchable.find_one_and_replace({"doi": doc["doi"]}, stripped_down)
        else:
            db.entries_searchable.insert_one(stripped_down)
