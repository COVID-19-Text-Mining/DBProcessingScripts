from mongoengine import connect, DoesNotExist
from elsevier import UnparsedElsevierDocument
from google_form_submissions import UnparsedGoogleFormSubmissionDocument
from litcovid import UnparsedLitCovidCrossrefDocument, UnparsedLitCovidPubmedXMLDocument
from biorxiv import UnparsedBiorxivDocument
from cord19 import UnparsedCORD19CustomDocument, UnparsedCORD19CommDocument, UnparsedCORD19NoncommDocument, UnparsedCORD19XrxivDocument
from pho import UnparsedPHODocument
from dimensions import UnparsedDimensionsDataDocument, UnparsedDimensionsPubDocument, UnparsedDimensionsTrialDocument
from lens_patents import UnparsedLensDocument
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

unparsed_collection_list = [UnparsedDimensionsDataDocument,
     UnparsedDimensionsPubDocument,
     UnparsedDimensionsTrialDocument,
     UnparsedLensDocument,
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

    try:
        parsed_document = document.parsed_document
    except DoesNotExist:
        parsed_document = None

    if parsed_document is None or document.last_updated > parsed_document._bt or parsed_document.version < parsed_document.latest_version:
        if parsed_document is None:
           parsed_document = document.parse()
        else:
            new_doc = document.parse()
            parsed_document.delete()
            parsed_document = new_doc
        document.parsed_document = parsed_document
        parsed_document.find_missing_ids()
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
    print('parsed')


with Parallel(n_jobs=32) as parallel:
    parallel(delayed(parse_documents)(document) for collection in unparsed_collection_list for document in grouper(500, collection.objects))

build_entries()