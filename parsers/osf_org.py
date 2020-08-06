import os
from datetime import datetime

from mongoengine import DynamicDocument, ReferenceField, DateTimeField, connect, StringField

from base import Parser, VespaDocument, indexes
from utils import clean_title

latest_version = 1


class OSFOrgDocument(VespaDocument):
    meta = {"collection": "Scraper_osf_org_parsed_vespa",
            "indexes": indexes
            }

    latest_version = latest_version
    unparsed_document = ReferenceField('UnparsedOSFOrgDocument', required=True)


class OSFOrgParser(Parser):
    def _parse_doi(self, doc):
        return doc['doi']

    def _parse_title(self, doc):
        return clean_title(doc['title'])

    def _parse_authors(self, doc):
        return [{'name': x} for x in doc['contributors']]

    def _parse_journal(self, doc):
        return doc['sources'][0]

    _parse_issn = _parse_journal_short = \
        _parse_body_text = _parse_references = _parse_cited_by = \
        _parse_category_human = _parse_pmcid = _parse_pubmed_id = _parse_who_covidence = \
        lambda self, doc: None

    def _parse_publication_date(self, doc):
        return doc.get('date_published', datetime.now())

    _parse_has_year = _parse_has_month = _parse_has_day = lambda self, doc: True

    def _parse_abstract(self, doc):
        return doc['description']

    def _parse_origin(self, doc):
        return "Scraper_osf_org"

    def _parse_source_display(self, doc):
        return doc['sources'][0]

    def _parse_last_updated(self, doc):
        return doc['last_updated']

    def _parse_has_full_text(self, doc):
        return False

    def _parse_link(self, doc):
        return f'https://doi.org/{doc["doi"]}'

    def _parse_keywords(self, doc):
        return doc['tags']

    def _parse_summary_human(self, doc):
        """ Returns the human-written summary of a document as a <class 'list'> of <class 'str'>"""
        return None

    def _parse_is_preprint(self, doc):
        return True

    def _parse_is_covid19(self, doc):
        return None

    def _parse_license(self, doc):
        return 'OSF'

    def _parse_version(self, doc):
        return latest_version

    def _parse_copyright(self, doc):
        return None

    def _parse_document_type(self, doc):
        return 'paper'


class UnparsedOSFOrgDocument(DynamicDocument):
    meta = {
        "collection": "Scraper_osf_org"
    }

    parser = OSFOrgParser()

    parsed_class = OSFOrgDocument

    parsed_document = ReferenceField(OSFOrgDocument, required=False)

    last_updated = DateTimeField(db_field="last_updated")

    def parse(self):
        parsed_document = self.parser.parse(self.to_mongo())
        parsed_document['_bt'] = datetime.now()
        parsed_document['unparsed_document'] = self
        return OSFOrgDocument(**parsed_document)


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

    for i in UnparsedOSFOrgDocument.objects.all():
        parsed = i.parse()
        parsed.save()
        break
