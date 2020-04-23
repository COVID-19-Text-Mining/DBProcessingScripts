# -*- coding: utf-8 -*-
"""
Created on Fri Apr  3 13:36:44 2020

@author: elise
"""

import re
import unicodedata


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


def clean_title(title):
    """
    @ Yuxing Fei
    This function will:
        1. Replace all the characters to its canonical form
        2. Remove urls
        3. Remove abnormal tokens,
           for example, too many continuous digits or punctuations

    If the input title is None or empty string,
        the function will return the raw title
    """
    if not title:
        return title

    title = unicodedata.normalize('NFKC', title)
    title = re.sub(r"[—–]", "-", title)
    title = re.sub(r"(?<=\s)[^A-z0-9&!@#$%^*,./;=|'+:\"()\-\s](?=\s)|(https?://\s?|www\.)[^\s)]+", " ", title)
    title = re.sub(r"-NC-ND license", "", title)
    title = re.sub(r"\(\s+\)", "", title)
    tokens = title.split()
    new_tokens = []
    noise = []
    for token in tokens:
        if re.fullmatch(r"[\u0370-\u03ffA-z(]?[\w\-,.;:()?/]{2,}", token) is not None:
            if len(noise) <= 5:
                new_tokens.extend(noise)  # actually not noise
                noise = []
            else:
                noise = []  # throw away noise
            if token:
                new_tokens.append(token)

        else:
            noise.append(token)
    if len(noise) <= 2:
        new_tokens.extend(noise)

    return " ".join(new_tokens)