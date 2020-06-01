from mongoengine import (
    connect, Document, EmbeddedDocumentField,
    StringField, ListField,
    EmbeddedDocument, EmailField, ValidationError, DateTimeField, DynamicEmbeddedDocument, BooleanField, IntField)
from parsers.base import VespaDocument

class MetadataDocument(VespaDocument):
    link = StringField(default=None)
    publication_date = DateTimeField(default=None)
    has_year = BooleanField(default=None)
    has_month = BooleanField(default=None)
    has_day = BooleanField(default=None)
