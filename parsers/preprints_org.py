import os
from datetime import datetime

from mongoengine import (
    DynamicDocument, ReferenceField, DateTimeField, StringField,
    IntField, LongField, ListField, BooleanField, connect)

from base import Parser, VespaDocument, indexes

latest_version = 1


class PreprintsOrgDocument(VespaDocument):
    meta = {
        "collection": "Scraper_preprints_org_parsed_vespa",
        "indexes": indexes
    }

    latest_version = latest_version
    unparsed_document = ReferenceField('UnparsedPreprintsOrgDocument', required=True)


class PreprintsOrgParser(Parser):
    """preprints.org"""

    def _parse_title(self, doc):
        return doc['Title']

    def _parse_authors(self, doc):
        return [{'name': x} for x in doc['Authors']]

    def _parse_journal(self, doc):
        return 'preprints.org'

    def _parse_publication_date(self, doc):
        return doc['Publication_Date']

    def _parse_abstract(self, doc):
        return doc['Abstract']

    def _parse_origin(self, doc):
        return "Scraper_preprints_org"

    def _parse_source_display(self, doc):
        return 'preprints.org collection on COVID-19 and SARS-CoV-2'

    def _parse_link(self, doc):
        s = doc['Doi'][len('10.20944/preprints'):]
        paper_id, v = s.rsplit('.', maxsplit=1)

        return f'https://www.preprints.org/manuscript/{paper_id}/{v}'

    def _parse_document_type(self, doc):
        return 'paper'

    def _parse_last_updated(self, doc):
        return doc['last_updated']

    def _parse_has_full_text(self, doc):
        return doc['fulltext'] is not None

    def _parse_body_text(self, doc):
        if doc['fulltext']:
            return [{'section_heading': None, 'text': t} for t in doc['fulltext']]
        else:
            return []

    def _parse_doi(self, doc):
        return doc['Doi']

    def _parse_keywords(self, doc):
        return doc['Keywords']

    def _parse_license(self, doc):
        return 'CC-BY 4.0'

    _parse_category_human = _parse_summary_human = \
        _parse_issn = _parse_journal_short = _parse_references = \
        _parse_cited_by = _parse_pmcid = _parse_pubmed_id = \
        _parse_who_covidence = lambda self, doc: None

    _parse_has_year = _parse_has_month = _parse_has_day = \
        _parse_is_preprint = _parse_is_covid19 = lambda self, doc: True

    def _parse_version(self, doc):
        return latest_version

    def _parse_copyright(self, doc):
        return "This is an open access article distributed under the Creative Commons Attribution " \
               "License which permits unrestricted use, distribution, and reproduction in any medium, " \
               "provided the original work is properly cited."


class PreprintsOrgFullText(DynamicDocument):
    meta = {
        "collection": "Scraper_preprints_org_fs.files"
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

    def get_paragraphs(self):
        if not self.pdf_extraction_success:
            return None
        return [x['text'] for x in self.pdf_extraction_plist]


class UnparsedPreprintsOrgDocument(DynamicDocument):
    meta = {
        "collection": "Scraper_preprints_org"
    }

    parser = PreprintsOrgParser()
    parsed_class = PreprintsOrgDocument

    fulltext = ReferenceField(PreprintsOrgFullText, db_field='PDF_gridfs_id')
    parsed_document = ReferenceField(PreprintsOrgDocument, required=False)
    last_updated = DateTimeField()

    def parse(self):
        doc = self.to_mongo()
        doc['fulltext'] = self.fulltext.get_paragraphs()

        parsed_document = self.parser.parse(doc)
        parsed_document['_bt'] = datetime.now()
        parsed_document['unparsed_document'] = self
        return PreprintsOrgDocument(**parsed_document)


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

    for i in UnparsedPreprintsOrgDocument.objects.all():
        parsed = i.parse()
        parsed.save()
        break
