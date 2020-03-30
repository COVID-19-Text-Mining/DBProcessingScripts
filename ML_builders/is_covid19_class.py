#!/usr/bin/env python
# coding: utf-8

# In[11]:



import os
import sys
from pymongo import MongoClient
import json
from tqdm import tqdm_notebook
from fuzzysearch import find_near_matches
from difflib import SequenceMatcher as SM
import datetime

client = MongoClient(
    host=os.environ["COVID_HOST"], 
    username=os.environ["COVID_USER"], 
    password=os.environ["COVID_PASS"],
    authSource=os.environ["COVID_DB"])

db = client[os.environ["COVID_DB"]]


# In[35]:

# last_keyword_sweep = db.metadata.find_one({'data': 'last_keyword_sweep'})['datetime']
entries = db.entries.find({}, projection = ["abstract", 'title', "keywords", "keywords_ML", 'category_human', 'is_covid19', 'body_text'])

# In[48]:

def is_covid19(entry):

    for covid_word in covid19_words:
        for key in ['abstract', 'title']:
            if key in entry.keys():
                if isinstance(entry[key], list):
                    text = " ".join(entry[key])
                else:
                    text = entry[key]
                if text is not None:
                    #"provide" is really painful here
                    if len([m for m in find_near_matches(covid_word, text, max_l_dist=1, max_substitutions=1) if m.matched != "ovid"]) > 0:
                        return True
        for key in ['keywords_ML', 'keywords']:
            if key in entry.keys():
                if isinstance(entry[key], list):
                    text = " ".join(entry[key])
                else:
                    text = entry[key]
                if text is not None:
                    if len(find_near_matches(covid_word, text, max_l_dist=1, max_substitutions=1)) > 0:
                        return True

        if 'body_text' in entry.keys():
            if entry['body_text'] is not None:
                for e in entry['body_text']:
                    if 'Text' in e.keys() and e['Text'] is not None:
                        if len([m for m in find_near_matches(covid_word, text, max_l_dist=1, max_substitutions=1) if m.matched != "ovid"]) > 0:
                            return True
    return False

covid_count = 0
for i, entry in enumerate(entries):

    if i%1000 == 0:
        print((i, covid_count))
    covid19_words = ["COVID-19", "SARS-CoV2", "sars-cov-2", "nCoV-2019", "covid19", "sarscov2", "ncov2019", "covid 19", "sars cov2", "ncov 2019", "severe acute respiratory syndrome coronavirus 2", "Wuhan seafood market pneumonia virus", "Coronavirus disease", "covid", "wuhan virus", "HCoV-19"]
    # if 'category_human' in entry.keys() and not "COVID-19/SARS-CoV2/nCoV-2019" in entry['category_human']:
        # print(entry['category_human'])
     # and entry['category_human'] == "COVID-19/SARS-CoV2/nCoV-2019":
    #     entry['is_covid19'] = (entry['category_human'] == "COVID-19/SARS-CoV2/nCoV-2019")
    # else:
    if len(entry['category_human']) > 0:
        entry['is_covid19'] = "COVID-19/SARS-CoV2/nCoV-2019" in entry['category_human']
    else:
        entry['is_covid19'] = is_covid19(entry)
    if entry['is_covid19']:
        covid_count += 1

# print(covid_count)
    db.entries.update_one({"_id": entry["_id"]}, {"$set": {"is_covid19": entry["is_covid19"], "last_updated": datetime.datetime.now()}})

# db.metadata.update_one({'data':"last_keyword_sweep"}, {"$set": {"datetime": datetime.datetime.now()}})
