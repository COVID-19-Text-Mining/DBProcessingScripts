import os
from collections import OrderedDict
from datetime import datetime
from typing import Optional

from mongoengine import (
    DynamicDocument, ReferenceField, DateTimeField, StringField,
    IntField, LongField, ListField, BooleanField, connect)

from base import Parser, VespaDocument, indexes
from utils import clean_title

latest_version = 1

class PHODocument(VespaDocument):
    meta = {
        "collection": "Scraper_publichealthontario_parsed_vespa",
        "indexes": indexes
    }

    latest_version = latest_version
    unparsed_document = ReferenceField('UnparsedPHODocument', required=True)


class PHOParser(Parser):
    """Public health ontario"""

    def _parse_title(self, doc):
        return 'Synopsis: Review of "%s"' % clean_title(doc['Title'])

    def _parse_authors(self, doc):
        return [{'name': x} for x in doc['Authors']]

    def _parse_journal(self, doc):
        return 'Public Health Ontario Synopsis'

    def _parse_publication_date(self, doc):
        return datetime.strptime(doc['Date_Created'].replace(',', '').strip(), '%B %d %Y')

    def _parse_abstract(self, doc):
        paragraphs = [doc['Desc']]
        if doc['synopsis'] is not None and 'one-minute summary' in doc['synopsis']:
            sub_p = [x["text"] for x in doc["synopsis"]["one-minute summary"]]
            paragraphs.append(
                f'One-minute summary: {" ".join(sub_p)}'
            )
        return '\n'.join(paragraphs)

    def _parse_origin(self, doc):
        return "Scraper_publichealthontario"

    def _parse_source_display(self, doc):
        return 'Public Health Ontario COVID-19 Synopsis'

    def _parse_link(self, doc):
        return doc['Synopsis_Link']

    def _parse_document_type(self, doc):
        return 'synopsis'

    def _parse_last_updated(self, doc):
        """ Returns when the entry was last_updated as a <class 'datetime.datetime'>. Note
        this should probably not be the _bt field in a Parser."""
        return doc['last_updated']

    def _parse_has_full_text(self, doc):
        return doc['synopsis'] is not None

    def _parse_body_text(self, doc):
        if doc['synopsis'] is None:
            return None

        paragraphs = []
        for field, plist in doc['synopsis'].items():
            for p in plist:
                paragraphs.append({
                    'section_heading': field.capitalize(),
                    'text': p['text']
                })
        return paragraphs

    _parse_doi = _parse_license = \
        _parse_category_human = _parse_keywords = _parse_summary_human = \
        _parse_issn = _parse_journal_short = _parse_references = \
        _parse_cited_by = _parse_pmcid = _parse_pubmed_id = \
        _parse_who_covidence = lambda self, doc: None

    _parse_has_year = _parse_has_month = _parse_has_day = \
        _parse_is_preprint = _parse_is_covid19 = lambda self, doc: True

    def _parse_version(self, doc):
        return latest_version

    def _parse_copyright(self, doc):
        return "The application and use of this document is the responsibility of the user. " \
               "PHO assumes no liability resulting from any such application or use. This document " \
               "may be reproduced without permission for non-commercial purposes only and provided " \
               "that appropriate credit is given to PHO. No changes and/or modifications may be made " \
               "to this document without express written permission from PHO."


class PHOFullText(DynamicDocument):
    meta = {
        "collection": "Scraper_publichealthontario_fs.files"
    }
    filename = StringField(required=True)
    md5 = StringField(required=True)
    chunkSize = IntField(required=True)
    length = LongField(required=True)
    uploadDate = DateTimeField(required=True)

    pdf_extraction_success = BooleanField()
    pdf_extraction_plist = ListField()
    pdf_extraction_exec = StringField()
    pdf_extraction_version = StringField()
    parsed_date = DateTimeField()

    def get_synopsis(self) -> Optional[OrderedDict]:
        sections = {}
        last_sec = None
        for p in self.pdf_extraction_plist:
            is_heading = 18 < p['bbox'][3] - p['bbox'][1] and p['bbox'][2] - p['bbox'][0] < 230
            if is_heading:
                last_sec = p['text'].lower()
                sections[last_sec] = []
            elif last_sec is not None:
                sections[last_sec].append(p)

        synopsis = OrderedDict()
        for field in ['one-minute summary', 'additional information', 'pho reviewers comments']:
            if field in sections:
                synopsis[field] = sections[field]

        return synopsis or None


class UnparsedPHODocument(DynamicDocument):
    meta = {
        "collection": "Scraper_publichealthontario"
    }

    parser = PHOParser()
    parsed_class = PHODocument

    fulltext = ReferenceField(PHOFullText, db_field='PDF_gridfs_id')
    parsed_document = ReferenceField(PHODocument, required=False)
    last_updated = DateTimeField()

    def parse(self):
        doc = self.to_mongo()
        doc['synopsis'] = self.fulltext.get_synopsis()

        parsed_document = self.parser.parse(doc)
        parsed_document['_bt'] = datetime.now()
        parsed_document['unparsed_document'] = self
        return PHODocument(**parsed_document)


if __name__ == '__main__':
    # Test
    connect(
        db=os.getenv("COVID_DB"),
        name=os.getenv("COVID_DB"),
        host=os.getenv("COVID_HOST"),
        username=os.getenv("COVID_USER"),
        password=os.getenv("COVID_PASS"),
        authentication_source=os.getenv("COVID_DB"),
    )

    for i in UnparsedPHODocument.objects.all():
        parsed = i.parse()
        parsed.save()
        break
