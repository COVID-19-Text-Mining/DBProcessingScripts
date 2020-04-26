from base import Parser, VespaDocument, indexes
import json
from datetime import datetime
import requests
from utils import clean_title, find_cited_by, find_references, find_remaining_ids
from pprint import PrettyPrinter
from mongoengine import DynamicDocument, ReferenceField, DateTimeField

latest_version = 3

class GoogleFormSubmissionDocument(VespaDocument):
    meta = {"collection": "google_form_submissions_parsed_vespa",
            "indexes": indexes
    }

    latest_version = latest_version
    unparsed_document = ReferenceField('UnparsedGoogleFormSubmissionDocument', required=True)

class GoogleSubmissionParser(Parser):
    """
    Parser for documents from Google submissions
    """

    def _parse_doi(self, doc):
        """ Returns the DOI of a document as a <class 'str'>"""
        if doc['doi']:
            return doc['doi']
        return None

    def _parse_title(self, doc):
        """ Returns the title of a document as a <class 'str'>"""
        if 'title' in doc.keys() and len(doc['title']) > 0:
            if isinstance(doc['title'], list):
                return doc['title'][0]
            elif isinstance(doc['title'], str):
                return doc['title']
        return None

    def _parse_authors(self, doc):
        """ Returns the authors of a document as a <class 'list'> of <class 'dict'>.
        Each element in the authors list should have a "name" field with the author's
        full name (e.g. John Smith or J. Smith) as a <class 'str'>.
        """
        if 'authors' in doc.keys() and doc['authors']:
            authors = []
            for a in doc['authors']:
                author = {"name": a['name']}
                if a['affiliation'] != []:
                    author['institution'] = a['affiliation'][0]['name']
                authors.append(author)
            return authors
        return None

    def _parse_journal(self, doc):
        """ Returns the journal of a document as a <class 'str'>. """
        if 'journal' in doc.keys() and len(doc['journal']) > 0:
            return doc['journal'][0]
        return None

    def _parse_issn(self, doc):
        """ Returns the ISSN and (or) EISSN of a document as a <class 'list'> of <class 'str'> """
        if 'crossref_raw_result' in doc.keys() and 'ISSN' in doc['crossref_raw_result']['message'].keys():
            return doc['crossref_raw_result']['message']['ISSN'][0]
        return None

    def _parse_journal_short(self, doc):
        """ Returns the shortend journal name of a document as a <class 'str'>, if available.
         e.g. 'Comp. Mat. Sci.' """
        if 'crossref_raw_result' in doc.keys() and 'short-container-title' in doc['crossref_raw_result']['message'].keys():
            short_title = doc['crossref_raw_result']['message']['short-container-title']
            if isinstance(short_title, list):
                if len(short_title) > 0:
                    return short_title[0]
                else:
                    return None
            else:
                return short_title
        return None

    def _parse_publication_date(self, doc):
        """ Returns the publication_date of a document as a <class 'datetime.datetime'>"""
        if 'publication_date' in doc.keys():
            return doc['publication_date']
        return doc['last_updated']

    def _parse_has_year(self, doc):
        """ Returns a <class 'bool'> specifying whether a document's year can be trusted."""
        return 'publication_date' in doc.keys() and len(datetime.strftime(doc['publication_date'], '%Y-%m-%d')) >= 4

    def _parse_has_month(self, doc):
        """ Returns a <class 'bool'> specifying whether a document's month can be trusted."""
        return 'publication_date' in doc.keys() and len(datetime.strftime(doc['publication_date'], '%Y-%m-%d')) >= 7

    def _parse_has_day(self, doc):
        """ Returns a <class 'bool'> specifying whether a document's day can be trusted."""
        return 'publication_date' in doc.keys() and len(datetime.strftime(doc['publication_date'], '%Y-%m-%d')) == 10

    def _parse_abstract(self, doc):
        """ Returns the abstract of a document as a <class 'str'>"""
        if 'abstract' in doc.keys() and doc['abstract'] != None:
            return doc['abstract']
        elif 'crossref_raw_result' in doc.keys() and 'abstract' in doc['crossref_raw_result']['message'].keys():
            return doc['crossref_raw_result']['message']['abstract']
        return None

    def _parse_origin(self, doc):
        """ Returns the origin of the document as a <class 'str'>. Use the mongodb collection
        name for this."""
        return 'google_form_submissions'

    def _parse_source_display(self, doc):
        """ Returns the source of the document as a <class 'str'>. This is what will be
        displayed on the website, so use something people will recognize properly and
        use proper capitalization."""
        return 'COVIDScholar Submission'

    def _parse_last_updated(self, doc):
        """ Returns when the entry was last_updated as a <class 'datetime.datetime'>. Note
        this should probably not be the _bt field in a Parser."""
        return datetime.now()

    def _parse_has_full_text(self, doc):
        """ Returns a <class 'bool'> specifying if we have the full text."""
        return False

    def _parse_body_text(self, doc):
        """ Returns the body_text of a document as a <class 'list'> of <class 'dict'>.
        This should be a list of objects of some kind. Seems to be usually something like
        {'section_heading':  <class 'str'>,
         'text': <class 'str'>
         }
         """
        return None

    def _parse_references(self, doc):
        """ Returns the references of a document as a <class 'list'> of <class 'dict'>.
        This is a list of documents cited by the current document.
        """
        doi = self._parse_doi(doc)
        return find_references(doi)

    def _parse_cited_by(self, doc):
        """ Returns the citations of a document as a <class 'list'> of <class 'str'>.
        A list of DOIs of documents that cite this document.
        """
        doi = self._parse_doi(doc)
        return find_cited_by(doi)

    def _parse_link(self, doc):
        """ Returns the url of a document as a <class 'str'>"""
        if type(doc) == dict and'link' in doc.keys():
            return doc['link']
        elif self._parse_doi(doc):
            return 'https://doi.org/' + self._parse_doi(doc)
        return

    def _parse_category_human(self, doc):
        """ Returns the category_human of a document as a <class 'list'> of <class 'str'>"""
        return doc['category_human']

    def _parse_keywords(self, doc):
        """ Returns the keywords for a document from original source as a a <class 'list'> of <class 'str'>"""
        return doc['keywords']

    def _parse_summary_human(self, doc):
        """ Returns the human-written summary of a document as a <class 'list'> of <class 'str'>"""
        return [doc['summary_human']]

    def _parse_is_preprint(self, doc):
        """ Returns a <class 'bool'> specifying whether the document is a preprint.
        If it's not immediately clear from the source it's coming from, return None."""
        return None

    def _parse_is_covid19(self, doc):
        """ Returns a <class 'bool'> if we know for sure a document is specifically about COVID-19.
        If it's not immediately clear from the source it's coming from, return None."""
        return True

    def _parse_license(self, doc):
        """ Returns the license of a document as a <class 'str'> if it is specified in the original doc."""
        return None

    def _parse_cord_uid(self, doc):
        """ Returns the cord_uid of a document as a <class 'str'> if it is available."""
        return None

    def _parse_pmcid(self, doc):
        """ Returns the pmcid of a document as a <class 'str'>."""
        return find_remaining_ids(self._parse_doi(doc))['pmcid']

    def _parse_pubmed_id(self, doc):
        """ Returns the PubMed ID of a document as a <class 'str'>."""
        return find_remaining_ids(self._parse_doi(doc))['pubmed_id']

    def _parse_who_covidence(self, doc):
        """ Returns the who_covidence of a document as a <class 'str'>."""
        return None

    def _parse_version(self, doc):
        """ Returns the version of a document as a <class 'int'>."""
        return latest_version

    def _parse_copyright(self, doc):
        """ Returns the copyright notice of a document as a <class 'str'>."""
        return None

    def _parse_document_type(self, doc):
        """ Returns the document type of a document as a <class 'str'>.
        e.g. 'paper', 'clinical_trial', 'patent', 'news'. """
        return 'paper' if self._parse_doi(doc) is not None else ''

    def _preprocess(self, doc):
        """
        Preprocesses an entry from the google_form_submissions collection into a flattened
        metadata document.
        Args:
            doc:
        Returns:
        """
        return doc

    def _postprocess(self, doc, parsed_doc):
        """
        Post-process an entry to add any last-minute fields required. Gets called right before parsed doc is
        returned. Return parsed_doc if you don't want to make any changes.
        """
        return parsed_doc

class UnparsedGoogleFormSubmissionDocument(DynamicDocument):
    meta = {"collection": "google_form_submissions"
    }

    parser = GoogleSubmissionParser()

    parsed_class = GoogleFormSubmissionDocument

    parsed_document = ReferenceField(GoogleFormSubmissionDocument, required=False)

    last_updated = DateTimeField(db_field="last_updated")

    def parse(self):
        parsed_document = self.parser.parse(self.to_mongo())
        parsed_document['_bt'] = datetime.now()
        parsed_document['unparsed_document'] = self
        return GoogleFormSubmissionDocument(**parsed_document)
