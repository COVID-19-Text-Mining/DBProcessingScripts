from mongoengine import connect, DoesNotExist
from elsevier import UnparsedElsevierDocument
from google_form_submissions import UnparsedGoogleFormSubmissionDocument
from litcovid import UnparsedLitCovidDocument
from biorxiv import UnparsedBiorxivDocument
from cord19 import UnparsedCORD19CustomDocument, UnparsedCORD19CommDocument, UnparsedCORD19NoncommDocument, UnparsedCORD19XrxivDocument
from pho import UnparsedPHODocument
from dimensions import UnparsedDimensionsDataDocument, UnparsedDimensionsPubDocument, UnparsedDimensionsTrialDocument
from lens_patents import UnparsedLensDocument
from chemrxiv import UnparsedChemrxivDocument
from psyarxiv import UnparsedPsyarxivDocument
from nber import UnparsedNBERDocument
from preprints_org import UnparsedPreprintsOrgDocument
from ssrn import UnparsedSSRNDocument
from osf_org import UnparsedOSFOrgDocument
from datetime import datetime
from joblib import Parallel, delayed
import os
import json
import itertools
from entries import build_entries, EntriesDocument
from twitter_mentions import TwitterMentions
import pymongo
import os
from mongoengine.queryset.visitor import Q

client = pymongo.MongoClient(os.getenv("COVID_HOST"), username=os.getenv("COVID_USER"),
                             password=os.getenv("COVID_PASS"), authSource=os.getenv("COVID_DB"))
db = client[os.getenv("COVID_DB")]

db.entries_vespa2.delete_many({"publication_date": {"$exists": False}})

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
     UnparsedOSFOrgDocument,
     UnparsedSSRNDocument,
     UnparsedPreprintsOrgDocument,
     UnparsedNBERDocument,
     UnparsedPsyarxivDocument,
     UnparsedChemrxivDocument,
     UnparsedDimensionsDataDocument,
     UnparsedDimensionsPubDocument,
     UnparsedDimensionsTrialDocument,
     UnparsedLensDocument,
     UnparsedGoogleFormSubmissionDocument, 
     UnparsedPHODocument,
     UnparsedBiorxivDocument, 
     UnparsedLitCovidDocument, 
     UnparsedElsevierDocument,
     UnparsedCORD19CustomDocument,
     UnparsedCORD19CommDocument,
     UnparsedCORD19NoncommDocument, 
     UnparsedCORD19XrxivDocument,
     ]

def parse_document(document):

    try:
        parsed_document = document.parsed_document
    except DoesNotExist:
        parsed_document = None

    #print(parsed_document)
    if parsed_document is None or document.last_updated > parsed_document._bt or parsed_document.version < parsed_document.latest_version:
        try:
            if parsed_document is None:
                #print("parsing")
                parsed_document = document.parse()
            else:
                new_doc = document.parse()
                parsed_document.delete()
                parsed_document = new_doc
            document.parsed_document = parsed_document
            parsed_document.find_missing_ids()
            #try:
            parsed_document.save()
            document.save()
        except:
            pass

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
        #print(document)
    #print('parsed')


#for collection in unparsed_collection_list:
#    for document in collection.objects():
#        from pprint import pprint
#        if document.Doi == "10.1101/2020.06.14.20130666":
#            new_doc = document.parse()
#            new_doc.find_missing_ids()
#            pprint(new_doc.to_json())
#            
#        pprint(document.id)
#        parse_documents([document])
with Parallel(n_jobs=32) as parallel:
  parallel(delayed(parse_documents)(document) for collection in unparsed_collection_list for document in grouper(500, collection.objects))

build_entries()

#twitter_mentions = TwitterMentions()
#for doc in EntriesDocument.objects(Q(last_twitter_search__not__exists=True)):
#    twitter_mentions.query_doc(doc)
