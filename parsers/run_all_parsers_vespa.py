from mongoengine import connect
from elsevier import UnparsedElsevierDocument
from google_form_submissions import UnparsedGoogleFormSubmissionDocument
from litcovid import UnparsedLitCovidCrossrefDocument, UnparsedLitCovidPubmedXMLDocument
from biorxiv import UnparsedBiorxivDocument
from cord19 import UnparsedCORD19CustomDocument, UnparsedCORD19CommDocument, UnparsedCORD19NoncommDocument, UnparsedCORD19XrxivDocument
from pho import UnparsedPHODocument
from datetime import datetime
from joblib import Parallel, delayed
import os
import json
import itertools
from entries import build_entries

def init_mongoengine():
    connect(db=os.getenv("COVID_DB"),
            name=os.getenv("COVID_DB"),
            host=os.getenv("COVID_HOST"),
            username=os.getenv("COVID_USER"),
            password=os.getenv("COVID_PASS"),
            authentication_source=os.getenv("COVID_DB"),
            )

init_mongoengine()

unparsed_collection_list = [
     UnparsedGoogleFormSubmissionDocument, 
     UnparsedPHODocument,
     UnparsedElsevierDocument,
     UnparsedCORD19CustomDocument,
     UnparsedCORD19CommDocument,
     UnparsedCORD19NoncommDocument, 
     UnparsedCORD19XrxivDocument,
     UnparsedBiorxivDocument, 
     UnparsedLitCovidCrossrefDocument, 
     UnparsedLitCovidPubmedXMLDocument, 
     ]

def parse_document(document):

    parsed_document = document.parsed_document

    if parsed_document.version == 1 and parsed_document.origin == "Scraper_connect_biorxiv_org":
        print((parsed_document.origin, parsed_document.version))
    if parsed_document is None or document.last_updated > parsed_document._bt or parsed_document.version < parsed_document.latest_version:
        # print(document)
        print(parsed_document)
        if parsed_document is None:
           parsed_document = document.parse()
        else:
            print("MATCH!"+str(parsed_document.version))
            new_doc = document.parse()
            parsed_document.delete()
            parsed_document = new_doc
        document.parsed_document = parsed_document
        # print(parsed_document)
        parsed_document.save()
        document.save()

def grouper(n, iterable):
    it = iter(iterable)
    while True:
        chunk = tuple(itertools.islice(it, n))
        if not chunk:
            return
        yield chunk

def parse_documents(documents):
    init_mongoengine()
    # print("parsing")
    for document in documents:
        parse_document(document)


with Parallel(n_jobs=1) as parallel:
    parallel(delayed(parse_documents)(document) for collection in unparsed_collection_list for document in grouper(500, collection.objects))

build_entries()