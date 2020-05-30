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
from mongoengine import connect

from difflib import SequenceMatcher as SM
import datetime
from pprint import pprint

sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'parsers'))
from entries import EntriesDocument
from mongoengine.queryset.visitor import Q

nlp = spacy.load("en_core_sci_lg")
tr = pytextrank.TextRank()
nlp.add_pipe(tr.PipelineComponent, name="textrank", last=True)


# In[35]:

def init_mongoengine():
    connect(db=os.getenv("COVID_DB"),
            name=os.getenv("COVID_DB"),
            host=os.getenv("COVID_HOST"),
            username=os.getenv("COVID_USER"),
            password=os.getenv("COVID_PASS"),
            authentication_source=os.getenv("COVID_DB"),
            )

init_mongoengine()


entries = EntriesDocument.objects(Q(keywords_ML=[]) | Q(keywords_ML__exists=False))
# In[48]:
print(len(entries))

for entry in entries:
    # example text
    text=""
    entry_dict = entry.to_mongo()
    if 'abstract' in entry_dict.keys() and entry_dict['abstract'] is not None and len(entry_dict['abstract']) > 0:
        text = entry_dict["abstract"]
        human_keywords = entry_dict.get("keywords", [])
        if isinstance(human_keywords, list):
            human_keywords = [item for sublist in human_keywords for item in sublist]
        # add PyTextRank to the spaCy pipeline
        try:
            doc = nlp(text)
        except TypeError:
            pprint(entry_dict)
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
        entry.keywords_ML = ml_keywords

    covid19_words = ["COVID-19", "SARS-CoV2", "sars-cov-2", "nCoV-2019", "covid19", "sarscov2", "ncov2019", "covid 19", "sars cov2", "ncov 2019", "severe acute respiratory syndrome coronavirus 2", "Wuhan seafood market pneumonia virus", "Coronavirus disease", "covid", "wuhan virus"]
    if not entry.is_covid19:
        if 'category_human' in entry_dict.keys() and not entry_dict['category_human'] in ["", [], None]:
            entry.is_covid19 = (entry_dict['category_human'] == "COVID-19/SARS-CoV2/nCoV-2019")
        else:
            is_covid19 = False
            if 'keywords' in entry_dict.keys():
                if any([c.lower() in [e.lower() for e in entry_dict['keywords']] for c in covid19_words]):
                    is_covid19 = True
            if len(text) > 0:
                if any([c.lower() in text.lower() for c in covid19_words]):
                    is_covid19 = True
            if 'title' in entry_dict.keys() and entry_dict['title'] is not None:
                if any([c.lower() in entry_dict['title'].lower() for c in covid19_words]):
                    is_covid19 = True
            try:
                if any([c.lower() in e['text'].lower() for c in covid19_words for e in entry_dict['body_text']]):
                    is_covid19 = True
            except (KeyError, TypeError):
                pass

            entry.is_covid19 = is_covid19 or entry.is_covid19

            if 'publication_date' in entry_dict.keys() and (entry_dict['publication_date'] < datetime.datetime(year=2019,month=1,day=1) and entry_dict['has_year']):
                entry.is_covid19 = False

    # print(entry.is_covid19)
    #if 'is_covid19_ML' in entry_dict.keys():
        #if entry.is_covid19_ML < 0.5 and entry.is_covid19:
            #from pprint import pprint
            #pprint(entry_dict)
    entry.synced = False
    entry.save()

