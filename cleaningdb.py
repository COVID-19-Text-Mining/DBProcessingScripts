# -*- coding: utf-8 -*-
"""
Created on Fri Apr  3 13:36:44 2020

@author: elise
"""

import json
import re

with open('entries_04032020.json', 'r', encoding='utf8') as json_file:
    data = [json.loads(line) for line in json_file]


def remove_pre_proof(title):
    clean_title = title.replace('Journal Pre-proofs', ' ')
    clean_title = clean_title.replace('Journal Pre-proof', ' ')
    clean_title = clean_title.strip()
    if len(clean_title) == 0:
        clean_title = None
    return clean_title


def add_pre_proof_and_clean(entry):
    if entry['title'] != None and 'Journal Pre-proof' in entry['title']:
        entry['is_pre_proof'] = True
        entry['title'] = remove_pre_proof(entry['title'])
        
    else:
        entry['is_pre_proof'] = False
        

def remove_html(abstract):
    #necessary to check this to avoid removing text between less than and greater than signs
    if abstract is not None and bool(re.search('<.*?>.*?</.*?>', abstract)):
        clean_abstract = re.sub('<.*?>', '', abstract)
        return clean_abstract
    else:
        return abstract

def clean_data(data):
    cleaned_data = data
    for i in cleaned_data:
        add_pre_proof_and_clean(i)
        i['abstract'] = remove_html('abstract')
        if i['journal'] == 'PLoS ONE':    
            i['journal'] = 'PLOS ONE'
    return cleaned_data

clean_data(data)
    
