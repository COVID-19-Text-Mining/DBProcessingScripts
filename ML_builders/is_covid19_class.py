import os
import sys
from pymongo import MongoClient
import json
from tqdm import tqdm_notebook
from fuzzysearch import find_near_matches
from difflib import SequenceMatcher as SM
import datetime
import spacy
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'parsers'))
from entries import EntriesDocument
from mongoengine import connect

def init_mongoengine():
    connect(db=os.getenv("COVID_DB"),
            name=os.getenv("COVID_DB"),
            host=os.getenv("COVID_HOST"),
            username=os.getenv("COVID_USER"),
            password=os.getenv("COVID_PASS"),
            authentication_source=os.getenv("COVID_DB"),
            )

init_mongoengine()

covid19_classifier = spacy.load("./COVID19_model")

# In[35]:

entries = EntriesDocument.objects(is_covid19_ML__exists=False)
# In[48]:

def is_covid19_model(entry):
    # Returns float equal to relevancy score given by spacy model (between 0-1)
    if 'abstract' in entry.keys() and type(entry['abstract']) is str: # run model over abstract
        doc_score = covid19_classifier(entry['abstract']).cats
        return float(doc_score['COVID19'])
    elif 'body_text' in entry.keys() and type(entry['body_text']) is list and len(entry['body_text']) > 0: # run model over body text if no abstract
        doc_scores = []
        for section in entry['body_text']:
            if 'text' in section.keys():
                doc_score = covid19_classifier(section['text']).cats['COVID19']
                doc_scores.append(doc_score)
            else:
                return
        return(max(doc_scores)) # return largest score from list of section scores
    return


covid_count = 0
for i, entry in enumerate(entries):

    if i%1000 == 0:
        print((i, covid_count))
    if is_covid19_model(entry.to_mongo()) is not None:
        entry.is_covid19_ML = is_covid19_model(entry.to_mongo()) # returns float value for score from model 
    print(entry.is_covid19_ML)

    entry.save()