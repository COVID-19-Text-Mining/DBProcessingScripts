import os
from datetime import datetime

from mongoengine import (
    DynamicDocument, ReferenceField, DateTimeField, StringField,
    IntField, LongField, ListField, BooleanField, connect)

from base import Parser, VespaDocument, indexes

latest_version = 1


class SSRNDocument(VespaDocument):
    meta = {
        "collection": "Scraper_papers_ssrn_com_parsed_vespa",
        "indexes": indexes
    }

    latest_version = latest_version
    unparsed_document = ReferenceField('UnparsedSSRNDocument', required=True)


class SSRNParser(Parser):
    """ssrn.com"""

    def _parse_title(self, doc):
        return doc['Title']

    def _parse_authors(self, doc):
        return [{'name': x['Name']} for x in doc['Authors']]

    def _parse_journal(self, doc):
        return 'SSRN'

    def _parse_publication_date(self, doc):
        return doc['Publication_Date']

    def _parse_abstract(self, doc):
        return doc['Abstract']

    def _parse_origin(self, doc):
        return "Scraper_papers_ssrn_com"

    def _parse_source_display(self, doc):
        return 'Social Science Research Network'

    def _parse_link(self, doc):
        return doc['Link']

    def _parse_document_type(self, doc):
        return 'paper'

    def _parse_last_updated(self, doc):
        return doc['last_updated']

    def _parse_has_full_text(self, doc):
        return False

    def _parse_body_text(self, doc):
        return None

    def _parse_doi(self, doc):
        return doc['Doi']

    def _parse_license(self, doc):
        return '© %d by the authors' % (doc['Publication_Date'].year,)

    _parse_category_human = _parse_keywords = _parse_summary_human = \
        _parse_issn = _parse_journal_short = _parse_references = \
        _parse_cited_by = _parse_pmcid = _parse_pubmed_id = \
        _parse_who_covidence = lambda self, doc: None

    # FIXME: is_covid19?
    def _parse_is_covid19(self, doc):
        return None

    _parse_has_year = _parse_has_month = _parse_has_day = \
        _parse_is_preprint = lambda self, doc: True

    def _parse_version(self, doc):
        return latest_version

    def _parse_copyright(self, doc):
        return None


class UnparsedSSRNDocument(DynamicDocument):
    meta = {
        "collection": "Scraper_papers_ssrn_com"
    }

    parser = SSRNParser()
    parsed_class = SSRNDocument

    parsed_document = ReferenceField(SSRNDocument, required=False)
    last_updated = DateTimeField()

    def parse(self):
        doc = self.to_mongo()

        parsed_document = self.parser.parse(doc)
        parsed_document['_bt'] = datetime.now()
        parsed_document['unparsed_document'] = self
        return SSRNDocument(**parsed_document)


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

    for i in UnparsedSSRNDocument.objects.all():
        parsed = i.parse()
        parsed.save()
        break
