from base import Parser, VespaDocument, indexes
from mongoengine.queryset.visitor import Q
import json
import re
from datetime import datetime
import requests
from utils import clean_title, find_cited_by, find_references
from elsevier import ElsevierDocument
from google_form_submissions import GoogleFormSubmissionDocument
from litcovid import LitCovidDocument
from biorxiv import BiorxivDocument
from cord19 import CORD19Document
from pho import PHODocument
from dimensions import DimensionsDocument
from lens_patents import LensPatentDocument
from chemrxiv import ChemrxivDocument
from psyrxiv import PsyrxivDocument
from mongoengine import ListField, GenericReferenceField, DoesNotExist, DictField, MultipleObjectsReturned, FloatField, StringField, BooleanField
import re
import os
import pymongo

client = pymongo.MongoClient(os.getenv("COVID_HOST"), username=os.getenv("COVID_USER"),
                             password=os.getenv("COVID_PASS"), authSource=os.getenv("COVID_DB"))
db = client[os.getenv("COVID_DB")]


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
    keywords_ML = ListField(StringField(required=True), default=lambda: [])
    synced = BooleanField(default=False)

entries_keys = [k for k in EntriesDocument._fields.keys() if (k[0] != "_")]

def find_matching_doc(doc):

    #This could definitely be better but I can't figure out how to mangle mongoengine search syntax in the right way
    doi = doc['doi'] if (doc['doi'] is not None) and (doc['doi'] != "") else "____"
    pubmed_id = doc['pubmed_id'] if doc['pubmed_id'] is not None else "____"
    pmcid = doc['pmcid'] if doc['pmcid'] is not None else "____"
    scopus_eid = doc['scopus_eid'] if doc['scopus_eid'] is not None else "____"
    title = doc['title'] if doc['title'] is not None and doc['title'] != "" else "________not_a_real_title_____"

    if doi[-3:-1] == ".v":
        doi = doi[:-3]

    pattern = re.compile("{}(\.v[0-9])?".format(re.escape(doi)))

    try:
        matching_doc = EntriesDocument.objects(Q(doi=pattern) | Q(pubmed_id=pubmed_id) | Q(pmcid=pmcid) | Q(scopus_eid=scopus_eid) | Q(title=title)).no_cache().get()
        return [matching_doc]
    except DoesNotExist:
        pass
    except MultipleObjectsReturned:
        return [d for d in EntriesDocument.objects(Q(doi=pattern) | Q(pubmed_id=pubmed_id) | Q(pmcid=pmcid) | Q(scopus_eid=scopus_eid) | Q(title=title)).no_cache()]
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
            if high_priority_doc.get(k,None) is not None and high_priority_doc[k] not in ["",
                                                                                                                   []]:
                merged_doc[k] = high_priority_doc[k]
            elif low_priority_doc.get(k,None) is not None and low_priority_doc[k] not in ["",
                                                                                                                  []]:
                merged_doc[k] = low_priority_doc[k]
            else:
                merged_doc[k] = None

        else:
            # Now merge the annotation categories into lists
            merged_category = []
            for doc in [high_priority_doc, low_priority_doc]:
                if isinstance(doc.get(k, None), str):
                    if not doc[k] in merged_category:
                        merged_category.append(doc[k])
                elif isinstance(doc.get(k,None), list):
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
    if merged_doc['authors'] is not None:
        for author in merged_doc['authors']:
            if  not 'name' in author.keys():
                name = "{}{}{}".format(author['first_name'] if 'first_name' in author.keys() else "", " " + author['middle_name'] if 'middle_name' in author.keys() else "", " " + author['last_name'] if 'last_name' in author.keys() else "",)
                author['name'] = name
            if ',' in author['name']:
                author['name'] = ' '.join(map(lambda x: x.strip(), reversed(author['name'].split(','))))


    if merged_doc['doi'] is not None and merged_doc['doi'][-3:-1] == ".v":
        merged_doc['doi'] = merged_doc['doi'][:-3]

    if merged_doc['publication_date'] is not None:
        if merged_doc['publication_date'] > datetime.now():
            merged_doc['publication_date'] = high_priority_doc['_bt']
        
    return merged_doc

parsed_collections = [
    PsyrxivDocument,
    # ChemrxivDocument,
    #DimensionsDocument,
    #LensPatentDocument,
    #BiorxivDocument,
    #GoogleFormSubmissionDocument,
    #PHODocument,
    #LitCovidDocument,
    CORD19Document,
    ElsevierDocument,
]

def build_entries():
    i=0
    #def find_matching_doc(doc):
    #    return []
    for collection in parsed_collections:
        print(collection)
        last_entries_builder_sweep = db.metadata.find_one({'data': 'last_entries_builder_sweep_vespa'})['datetime']

        #docs = [doc for doc in collection.objects(_bt__gte=last_entries_builder_sweep)]
        docs = [doc for doc in collection.objects()]
        # docs = collection.objects()
        for doc in docs:
            i+= 1
            if i%100 == 0:
                print(i)
            id_fields = [doc.to_mongo().get('doi', None), 
            doc.to_mongo().get('pubmed_id', None),
            doc.to_mongo().get('pmcid', None),
            ]
            matching_doc = find_matching_doc(doc)
            if len(matching_doc) == 1:
                insert_doc = EntriesDocument(**merge_documents(doc.to_mongo(), matching_doc[0].to_mongo()))
                insert_doc.id = matching_doc[0].id
                insert_doc.source_documents = matching_doc[0].source_documents
            elif len(matching_doc) > 1:
                insert_doc = merge_documents(matching_doc[0].to_mongo(), doc.to_mongo())
                insert_doc['source_documents'] = matching_doc[0].source_documents
                for d in matching_doc[1:]:
                    insert_doc = merge_documents(insert_doc, d.to_mongo())
                    insert_doc['source_documents'] = insert_doc['source_documents'] + d.source_documents
                    d.delete()
                insert_doc = EntriesDocument(**insert_doc)
                insert_doc.id = matching_doc[0].id                
            elif any([x is not None for x in id_fields]) or (doc.document_type in ['clinical_trial', 'patent']):
                insert_doc = EntriesDocument(**merge_documents(doc.to_mongo(), {'is_covid19': False}))
            else:
                insert_doc = None
            if insert_doc:
                insert_doc.source_documents.append(doc)
                insert_doc._bt = datetime.now()
                insert_doc.synced = False
                try:
                    insert_doc.save()
                except:
                    pass
    # db.metadata.update_one({'data': 'last_entries_builder_sweep_vespa'}, {"$set": {"datetime": datetime.now()}})

