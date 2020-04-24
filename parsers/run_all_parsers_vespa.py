from mongoengine import connect
from elsevier import UnparsedElsevierDocument
from google_form_submissions import UnparsedGoogleFormSubmissionDocument
from litcovid import UnparsedLitCovidCrossrefDocument, UnparsedLitCovidPubmedXMLDocument
from biorxiv import UnparsedBiorxivDocument
from cord19 import UnparsedCORD19CustomDocument, UnparsedCORD19CommDocument, UnparsedCORD19NoncommDocument, UnparsedCORD19XrxivDocument
from datetime import datetime
from joblib import Parallel, delayed
import os
import json
import itertools

def init_mongoengine():
    connect(db=os.getenv("COVID_DB"),
            name=os.getenv("COVID_DB"),
            host=os.getenv("COVID_HOST"),
            username=os.getenv("COVID_USER"),
            password=os.getenv("COVID_PASS"),
            authentication_source=os.getenv("COVID_DB"),
            )

init_mongoengine()

unparsed_collection_list = [UnparsedCORD19CustomDocument,
     UnparsedCORD19CommDocument,
     UnparsedCORD19NoncommDocument, 
     UnparsedCORD19XrxivDocument,
     UnparsedBiorxivDocument, 
     UnparsedLitCovidCrossrefDocument, 
     UnparsedLitCovidPubmedXMLDocument, 
     UnparsedGoogleFormSubmissionDocument, 
     UnparsedElsevierDocument,
     ]

def parse_document(document):

    parsed_document = document.parsed_document

    if parsed_document is None or document.last_updated > parsed_document._bt or parse_document.version < parsed_document.latest_version:
        # print(document)
        parsed_document = document.parse()
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
    print("parsing")
    for document in documents:
        parse_document(document)

for collection in unparsed_collection_list:
    with Parallel(n_jobs=32) as parallel:
        parallel(delayed(parse_documents)(document) for document in grouper(50, collection.objects))
