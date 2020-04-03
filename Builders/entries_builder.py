import os
import pymongo
import sys
import json
import datetime
import re
from pprint import pprint

client = pymongo.MongoClient(os.getenv("COVID_HOST"), username=os.getenv("COVID_USER"),
                             password=os.getenv("COVID_PASS"), authSource=os.getenv("COVID_DB"))
db = client[os.getenv("COVID_DB")]

#Rebuild the entire entries collection
rebuild = True
#Keys that are allowed in the entries database to keep it clean and minimal
entries_keys = ['title',
    'authors',
    'doi',
    'journal',
    'publication_date',
    'abstract',
    'origin',
    'last_updated',
    'body_text',
    'citations',
    'link',
    'category_human',
    'category_ML',
    'keywords',
    'keywords_ML',
    'summary_human',
    'summary_ML',
    'has_year',
    'has_month',
    'has_day',
    ]

def merge_documents(high_priority_doc, low_priority_doc):
    #Merge documents from two different source collections
    #Where they disagree, take the version from high_priority_doc

    merged_doc = dict()
    for k in entries_keys:
        #Treat human annotations separately - always merge them into a list
        if k not in ['summary_human', 'keywords', 'category_human', 'category_human']:
    
            #First fill in what we can from high_priority_doc
            if k in high_priority_doc.keys() and high_priority_doc[k] is not None and high_priority_doc[k] not in ["", []]:
                merged_doc[k] = high_priority_doc[k]
            elif k in low_priority_doc.keys() and low_priority_doc[k] is not None and low_priority_doc[k] not in ["", []]:
                merged_doc[k] = low_priority_doc[k]
            else:
                merged_doc[k] = None

        else:
            #Now merge the annotation categories into lists
            merged_category = []
            for doc in [high_priority_doc, low_priority_doc]:
                if k in doc.keys():
                    if isinstance(doc[k], str):
                        if not doc[k] in merged_category:
                            merged_category.append(doc[k])
                    elif isinstance(doc[k], list):
                        for e in doc[k]:
                            if not e in merged_category:
                                merged_category.append(e)

            merged_doc[k] = list(set([anno.strip() for anno in merged_category]))

    merged_doc['last_updated'] = datetime.datetime.now()

    for date_bool_key in ['has_day', 'has_month', 'has_year']:
        if date_bool_key not in merged_doc.keys():
            merged_doc[date_bool_key] = False

    #Common starting text to abstracts that we want to clean
    preambles = ["Abstract Background", "Abstract:", "Abstract", "Graphical Abstract Highlights d", "Resumen", "Résumé"]
    if 'abstract' in merged_doc.keys() and merged_doc['abstract'] is not None:
        if isinstance(merged_doc['abstract'], list):
            merged_doc['abstract'] = " ".join(merged_doc['abstract'])
            
        if 'a b s t r a c t' in merged_doc['abstract']:
            merged_doc['abstract'] = merged_doc['abstract'].split('a b s t r a c t')[1]

        try:
            merged_doc['abstract'] = re.sub('^<jats:title>*<\/jats:title>', '', merged_doc['abstract'])
            merged_doc['abstract'] = re.sub('<\/?jats:[^>]*>', '', merged_doc['abstract'])
        except TypeError:
            pprint(merged_doc['abstract'])
        for preamble in preambles:
            try:
                merged_doc['abstract'] = re.sub('^{} '.format(preamble), '', merged_doc['abstract'])
            except TypeError:
                pprint(merged_doc['abstract'])

    if 'title' in merged_doc.keys() and merged_doc['title'] is not None:
        if isinstance(merged_doc['title'], list):
            merged_doc['title'] = " ".join(merged_doc['title'])

    if 'journal' in merged_doc.keys() and merged_doc['journal'] is not None:
        if isinstance(merged_doc['journal'], list):
            merged_doc['journal'] = " ".join(merged_doc['journal'])

    return merged_doc

#Collections are listed in priority order
origin_collections = [
  'google_form_submissions',  
  'Scraper_connect_biorxiv_org',
  'Scraper_chemrxiv_org',
  'CORD_noncomm_use_subset',
  'CORD_comm_use_subset',
  'CORD_biorxiv_medrxiv',
  'CORD_custom_license',
  'CORD_metadata']

def document_priority_greater_than(doc1, doc2):
    #Compare the priority of doc1 and doc2 based on their origin collection
    #Return True if doc1 is higher priority to doc2
    #if the docs are from the same collection, the newest one is chosen

    #We don't handle the case where either doc doesn't have an origin
    #because we should definitely throw an exception when a ghost paper appears

    priority_dict = {c:-i for i,c in enumerate(origin_collections)}

    if priority_dict[doc1['origin']] > priority_dict[doc1['origin']]:
        return True
    elif doc1['origin'] == doc2['origin']:
        return doc1['last_updated'] >= doc2['last_updated']
    else:
        return False


parsed_collections = [a+"_parsed" for a in origin_collections]

query = {'doi': {"$exists": True}, 'title': {"$exists": True}}
if not rebuild:
    last_entries_builder_sweep = db.metadata.find_one({'data': 'last_entries_builder_sweep'})['datetime']
    query['_bt'] = {'$gte': last_entries_builder_sweep}

for collection in parsed_collections:
    print(collection)
    for doc in db[collection].find(query):
        #doi and title are mandatory

        existing_entry = db.entries_new.find_one({"doi": doc['doi']})
        if existing_entry:
        #Check to see if we already have a doc with this DOI in the entries collection
            if document_priority_greater_than(existing_entry, doc):
                #Figure out which doc has higher priority
                insert_doc = merge_documents(existing_entry, doc)
            else:
                insert_doc = merge_documents(doc, existing_entry)

        else:
            #otherwise use this to make a new entry
            insert_doc = merge_documents(doc, dict())
        db.entries_new.update_one({"doi": insert_doc['doi']}, {"$set": insert_doc}, upsert=True)

#We'll also check the raw google_form_submissions
#Uploading and parsing the PDFs takes a while, and
#so we do this to let people see their submission up sooner
#This is always the lowest priority
if not rebuild:
    query["last_updated"] = {'$gte': last_entries_builder_sweep}
    del(query['_bt'])

for doc in db.google_form_submissions.find(query):

    existing_entry = db.entries_new.find_one({"doi": doc['doi']})
    if existing_entry:
    #Check to see if we already have a doc with this DOI in the entries collection
    #This is always the lowest priority doc
        insert_doc = merge_documents(existing_entry, doc)
    else:
        #otherwise use this to make a new entry
        insert_doc = merge_documents(doc, dict())
        #Raw 'google form submissions' collection doesn't have an origin field
        insert_doc['origin'] = 'google_form_submissions'
    db.entries_new.update_one({"doi": insert_doc['doi']}, {"$set": insert_doc}, upsert=True)

#Finally, let's log that we've done a sweep so we don't have to go back over entries
db.metadata.update_one({'data': 'last_entries_builder_sweep'}, {"$set": {"datetime": datetime.datetime.now()}})