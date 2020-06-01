from mongoengine import (
    connect, Document, EmbeddedDocumentField,
    StringField, ListField,
    EmbeddedDocument, EmailField, ValidationError, DateTimeField, DynamicEmbeddedDocument, BooleanField, IntField)
from parsers.base import VespaDocument
import json
from pprint import pprint

class MetadataDocument(VespaDocument):
    link = StringField(default=None)
    publication_date = DateTimeField(default=None)
    has_year = BooleanField(default=None)
    has_month = BooleanField(default=None)
    has_day = BooleanField(default=None)

    @staticmethod
    def merge_docs(docs):
        if len(docs) == 0:
            return None
        result_doc = docs[0]
        for f in result_doc._fields:
            if result_doc[f] is None:
                for doc in docs[1:]:
                    if doc[f] is not None:
                        result_doc[f] = doc[f]
                        break
        return result_doc
