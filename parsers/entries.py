from base import Parser, VespaDocument, indexes
from mongoengine.queryset.visitor import Q
import json
import re
from datetime import datetime
import requests
from utils import clean_title, find_cited_by, find_references
from elsevier import ElsevierDocument
from google_form_submissions import GoogleFormSubmissionDocument
from litcovid import LitCovidCrossrefDocument, LitCovidPubmedDocument
from biorxiv import BiorxivDocument
from cord19 import CORD19Document
from pho import PHODocument
from dimensions import DimensionsDocument
from lens_patents import LensPatentDocument
from mongoengine import ListField, GenericReferenceField, DoesNotExist, DictField, MultipleObjectsReturned, FloatField, IntField

class EntriesDocument(VespaDocument):

    indexes = [
    'journal', 'journal_short',
    'publication_date',
    'has_full_text',
    'origin',
    'last_updated',
    'has_year', 'has_month', 'has_day',
    'is_preprint', 'is_covid19',
    'cord_uid',
    'who_covidence', 'version', 'copyright',
    'document_type',
    {"fields": ["doi",],
    "unique": True,
    "partialFilterExpression": {
        "doi": {"$type": "string"}
        }
    },
    {"fields": ["pmcid",],
    "unique": True,
    "partialFilterExpression": {
        "pmcid": {"$type": "string"}
        }
    },
    {"fields": ["pubmed_id",],
    "unique": True,
    "partialFilterExpression": {
        "pubmed_id": {"$type": "string"}
        }
    },
    {"fields": ["scopus_eid",],
    "unique": True,
    "partialFilterExpression": {
        "scopus_eid": {"$type": "string"}
        }
    },
    ]
    meta = {"collection": "entries_vespa",
            "indexes": indexes,
            "allow_inheritance": False
    }    

    source_documents = ListField(GenericReferenceField(), required=True)
    embeddings = DictField(default={})
    is_covid19_ML = FloatField()
    integer_id = IntField()

entries_keys = [k for k in EntriesDocument._fields.keys() if (k[0] != "_" and k not in ["source_documents", "embeddings", "is_covid19_ML", "integer_id"])]

def find_matching_doc(doc):
    #This could definitely be better but I can't figure out how to mangle mongoengine search syntax in the right way
    doi = doc['doi'] if doc['doi'] is not None else "_"
    pubmed_id = doc['pubmed_id'] if doc['pubmed_id'] is not None else "_"
    pmcid = doc['pmcid'] if doc['pmcid'] is not None else "_"
    scopus_eid = doc['scopus_eid'] if doc['scopus_eid'] is not None else "_"
    try:
        matching_doc = EntriesDocument.objects(Q(doi=doi) | Q(pubmed_id=pubmed_id) | Q(pmcid=pmcid) | Q(scopus_eid=scopus_eid)).no_cache().get()
        return [matching_doc]
    except DoesNotExist:
        pass
    except MultipleObjectsReturned:
        return [d for d in EntriesDocument.objects(Q(doi=doi) | Q(pubmed_id=pubmed_id) | Q(pmcid=pmcid) | Q(scopus_eid=scopus_eid)).no_cache()]
    return []

# -*- coding: utf-8 -*-
"""
Created on Fri Apr  3 13:36:44 2020

@author: elise
"""


def add_pre_proof_and_clean(entry):
    if entry['title'] != None and 'Journal Pre-proof' in entry['title']:
        entry['title'] = remove_pre_proof(entry['title'])
    return entry


def remove_pre_proof(title):
    clean_title = title.replace('Journal Pre-proofs', ' ')
    clean_title = clean_title.replace('Journal Pre-proof', ' ')
    clean_title = clean_title.strip()
    if len(clean_title) == 0:
        clean_title = None
    return clean_title


def remove_html(abstract):
    # necessary to check this to avoid removing text between less than and greater than signs
    if abstract is not None and bool(re.search('<.*?>.*?</.*?>', abstract)):
        clean_abstract = re.sub('<.*?>', '', abstract)
        return clean_abstract
    else:
        return abstract


def clean_data(doc):
    cleaned_doc = doc
    cleaned_doc = add_pre_proof_and_clean(cleaned_doc)
    cleaned_doc['abstract'] = remove_html(cleaned_doc['abstract'])
    if cleaned_doc['journal'] == 'PLoS ONE':
        cleaned_doc['journal'] = 'PLOS ONE'

    return cleaned_doc



def merge_documents(high_priority_doc, low_priority_doc):
    # Merge documents from two different source collections
    # Where they disagree, take the version from high_priority_doc

    merged_doc = dict()

    for k in entries_keys:
        # Treat human annotations separately - always merge them into a list
        if k not in ['summary_human', 'keywords', 'keywords_ML', 'category_human', 'category_human']:

            # First fill in what we can from high_priority_doc
            if high_priority_doc[k] is not None and high_priority_doc[k] not in ["",
                                                                                                                   []]:
                merged_doc[k] = high_priority_doc[k]
            elif low_priority_doc[k] is not None and low_priority_doc[k] not in ["",
                                                                                                                  []]:
                merged_doc[k] = low_priority_doc[k]
            else:
                merged_doc[k] = None

        else:
            # Now merge the annotation categories into lists
            merged_category = []
            for doc in [high_priority_doc, low_priority_doc]:
                if isinstance(doc[k], str):
                    if not doc[k] in merged_category:
                        merged_category.append(doc[k])
                elif isinstance(doc[k], list):
                    for e in doc[k]:
                        if not e in merged_category:
                            merged_category.append(e)

            merged_doc[k] = list(set([anno.strip() for anno in merged_category]))

    merged_doc['last_updated'] = datetime.now()

    for date_bool_key in ['has_day', 'has_month', 'has_year']:
        if date_bool_key not in merged_doc.keys():
            merged_doc[date_bool_key] = False

    # Common starting text to abstracts that we want to clean
    preambles = ["Abstract Background", "Abstract:", "Abstract", "Graphical Abstract Highlights d", "Resumen", "Résumé"]
    elsevier_preamble = "publicly funded repositories, such as the WHO COVID database with rights for unrestricted research re-use and analyses in any form or by any means with acknowledgement of the original source. These permissions are granted for free by Elsevier for as long as the COVID-19 resource centre remains active."
    preambles.append(elsevier_preamble)

    if merged_doc['abstract'] is not None:
        if isinstance(merged_doc['abstract'], list):
            merged_doc['abstract'] = " ".join(merged_doc['abstract'])

        if 'a b s t r a c t' in merged_doc['abstract']:
            merged_doc['abstract'] = merged_doc['abstract'].split('a b s t r a c t')[1]

        try:
            merged_doc['abstract'] = re.sub('^<jats:title>*<\/jats:title>', '', merged_doc['abstract'])
            merged_doc['abstract'] = re.sub('<\/?jats:[^>]*>', '', merged_doc['abstract'])
        except TypeError:
            pass
        for preamble in preambles:
            try:
                merged_doc['abstract'] = re.sub('^{}'.format(preamble), '', merged_doc['abstract'])
            except TypeError:
                pass

    if merged_doc['title'] is not None:
        if isinstance(merged_doc['title'], list):
            merged_doc['title'] = " ".join(merged_doc['title'])

    if merged_doc['journal'] is not None:
        if isinstance(merged_doc['journal'], list):
            merged_doc['journal'] = " ".join(merged_doc['journal'])

    merged_doc = clean_data(merged_doc)
    if merged_doc['abstract'] is not None:
        merged_doc['abstract'] = merged_doc['abstract'].strip()

    merged_doc['is_covid19'] = high_priority_doc['is_covid19'] or low_priority_doc['is_covid19']
    return merged_doc

parsed_collections = [
    DimensionsDocument,
    LensPatentDocument,
    BiorxivDocument,
    GoogleFormSubmissionDocument,
    PHODocument,
    LitCovidCrossrefDocument,
    LitCovidPubmedDocument,
    CORD19Document,
    ElsevierDocument,
]

def build_entries():
    i=0
    for collection in parsed_collections:
        print(collection)
        docs = [doc for doc in collection.objects]
        for doc in docs:
            i+= 1
            if i%1000 == 0:
                print(i)
            id_fields = [doc['doi'], 
            doc['pubmed_id'],
            doc['pmcid'],
            doc['scopus_eid'],
            ]
            matching_doc = find_matching_doc(doc)
            if len(matching_doc) == 1:
                insert_doc = EntriesDocument(**merge_documents(doc, matching_doc[0]))
                insert_doc.id = matching_doc[0].id
                insert_doc.source_documents = matching_doc[0].source_documents
            elif len(matching_doc) > 1:
                insert_doc = merge_documents(matching_doc[0], doc)
                insert_doc.source_documents = matching_doc[0].source_documents
                for d in matching_doc[1:]:
                    insert_doc = merge_documents(insert_doc, d)
                    insert_doc.source_documents = insert_doc.source_documents + d.source_documents
                    d.delete()
                insert_doc = EntriesDocument(**insert_doc)
                insert_doc.id = matching_doc[0].id                
            elif any([x is not None for x in id_fields]):
                insert_doc = EntriesDocument(**{k:v for k,v in doc.to_mongo().items() if k in entries_keys})
            else:
                insert_doc = None
            if insert_doc:
                insert_doc.source_documents.append(doc)
                insert_doc._bt = datetime.now()
                insert_doc.integer_id = int(insert_doc.id,16)
                insert_doc.save()
