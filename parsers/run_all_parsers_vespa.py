from mongoengine import connect
from elsevier import UnparsedElsevierDocument
from datetime import datetime
import os
import json

def init_mongoengine():
    connect(db=os.getenv("COVID_DB"),
            name=os.getenv("COVID_DB"),
            host=os.getenv("COVID_HOST"),
            username=os.getenv("COVID_USER"),
            password=os.getenv("COVID_PASS"),
            authentication_source=os.getenv("COVID_DB"),
            )

init_mongoengine()

unparsed_collection_list = [UnparsedElsevierDocument]

for collection in unparsed_collection_list:
    for document in collection.objects:
        parsed_document = document.parsed_document

        if parsed_document is None or document.last_updated > parsed_document._bt:
            print(document)
            parsed_document = document.parse()
            document.parsed_document = parsed_document
            print(parsed_document)
            parsed_document.save()
            document.save()