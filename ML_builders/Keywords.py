#!/usr/bin/env python
# coding: utf-8

# In[11]:


import scispacy
import spacy
import pytextrank

import os
import sys
from pymongo import MongoClient
import json
from tqdm import tqdm_notebook

from difflib import SequenceMatcher as SM
import datetime
from pprint import pprint

client = MongoClient(
    host=os.environ["COVID_HOST"], 
    username=os.environ["COVID_USER"], 
    password=os.environ["COVID_PASS"],
    authSource=os.environ["COVID_DB"])

db = client[os.environ["COVID_DB"]]
nlp = spacy.load("en_core_sci_lg")
tr = pytextrank.TextRank()
nlp.add_pipe(tr.PipelineComponent, name="textrank", last=True)


# In[35]:

last_keyword_sweep = db.metadata.find_one({'data': 'last_keyword_sweep'})['datetime']
entries = db.entries.find({'is_covid19': {"$exists": False}}, projection = ["abstract", 'title', "keywords", "keywords_ML", 'category_human', 'is_covid19', 'body_text'])

# In[48]:
# print(len(entries))

for entry in entries:

    # example text
    text=""
    if 'abstract' in entry.keys() and entry['abstract'] is not None and len(entry['abstract']) > 0:
        text = entry["abstract"]
        human_keywords = entry.get("keywords", [])
        if isinstance(human_keywords, list):
            human_keywords = [item for sublist in human_keywords for item in sublist]
        if "keywords_ML" and 'is_covid19' in entry:
            continue
        # add PyTextRank to the spaCy pipeline
        try:
            doc = nlp(text)
        except TypeError:
            pprint(entry)
        # examine the top-ranked phrases in the document
        ml_keywords = []
        for p in doc._.phrases:
            phrase = p.text
            phrase = phrase.replace("acute respiratory syndrome coronavirus", "SARS-CoV")
            phrase = phrase.replace("severe acute respiratory syndrome coronavirus", "SARS-CoV")
            phrase = phrase.replace("severe acute respiratory syndrome", "SARS")
            phrase = phrase.replace("middle eastern respiratory syndrome coronavirus", "MERS-CoV")
            phrase = phrase.replace("middle east respiratory syndrome coronavirus", "MERS-CoV")
            phrase = phrase.replace("middle east respiratory syndrome", "MERS")
            ml_keywords.append(phrase)
        entry["keywords_ML"] = ml_keywords

    else:
        entry['keywords_ML'] = ""
    covid19_words = ["COVID-19", "SARS-CoV2", "sars-cov-2", "nCoV-2019", "covid19", "sarscov2", "ncov2019", "covid 19", "sars cov2", "ncov 2019", "severe acute respiratory syndrome coronavirus 2", "Wuhan seafood market pneumonia virus", "Coronavirus disease", "covid", "wuhan virus"]
    if 'category_human' in entry.keys():
        entry['is_covid19'] = (entry['category_human'] == "COVID-19/SARS-CoV2/nCoV-2019")
    else:
        is_covid19 = False
        if 'keywords' in entry.keys():
            if any([c.lower() in [e.lower() for e in entry['keywords']] for c in covid19_words]):
                is_covid19 = True
        if 'keywords_ML' in entry.keys():
            if any([c.lower() in [e.lower() for e in entry['keywords_ML']] for c in covid19_words]):
                is_covid19 = True
        if len(text) > 0:
            if any([c.lower() in text.lower() for c in covid19_words]):
                is_covid19 = True
        if 'title' in entry.keys():
            if any([c.lower() in entry['title'].lower() for c in covid19_words]):
                is_covid19 = True
        try:
            if any([c.lower() in e['Text'].lower() for c in covid19_words for e in entry['body_text']]):
                is_covid19 = True
        except KeyError:
            pass

        entry['is_covid19'] = is_covid19

    # print(entry)
    db.entries.update_one({"_id": entry["_id"]}, {"$set": {"keywords_ML": entry["keywords_ML"], "is_covid19": entry["is_covid19"], "last_updated": datetime.datetime.now()}})

db.metadata.update_one({'data':"last_keyword_sweep"}, {"$set": {"datetime": datetime.datetime.now()}})
    # print(entry)
#         print("{:.4f} {:5d}  {}".format(p.rank, p.count, p.text))
#         print(p.chunks[0])
#     print(text)
#     if len(human_keywords):
#         print("HUMAN:")
#         print(human_keywords)
#     print("ROBOT:")
#     print(ml_keywords)
#     print("___________________________________________________________________________________________")



# In[49]:


# for entry in entries:
    # try:
    # except:
        # continue


# In[ ]:




