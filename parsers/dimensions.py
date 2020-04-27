from base import Parser, VespaDocument, indexes
import json
import re
from datetime import datetime
import requests
from utils import clean_title, find_cited_by, find_references, find_remaining_ids
from mongoengine import DynamicDocument, GenericReferenceField, DateTimeField, ReferenceField
from pprint import pprint

latest_version = 1

class DimensionsDocument(VespaDocument):

    meta = {"collection": "Dimensions_parsed_vespa",
            "indexes": indexes
    }

    latest_version = latest_version
    unparsed_document = GenericReferenceField(required=True)

class DimensionsParser(Parser):
    def __init__(self, collection):
        """
        Parser for documents from Dimensions COVID-19 publication, data set, and
        clinical trial collections.
        """
        self.collection_name = collection

    def _parse_doi(self, doc):
        """ Returns the DOI of a document as a <class 'str'>"""
        if 'doi' in doc.keys():
            return doc['doi']
        return None

    def _parse_title(self, doc):
        """ Returns the title of a document as a <class 'str'>"""
        if doc['title'] != '':
            return doc['title']
        return None

    def _parse_authors(self, doc):
        """ Returns the authors of a document as a <class 'list'> of <class 'dict'>.
        Each element in the authors list should have a "name" field with the author's
        full name (e.g. John Smith or J. Smith) as a <class 'str'>.
        """
        if 'authors' in doc.keys():
            if doc['authors'] != '':
                authors = doc['authors'].split('; ')
                authors_list = []
                for author in authors:
                    if ', ' in author:
                        first = author.split(', ')[1]
                        last = author.split(', ')[0]
                        if len(first) == 1:
                            authors_list.append({"first_name": first, "last_name": last})
                        else:
                            authors_list.append({"first_name": first, "last_name": last})
                    else:
                        authors_list.append({"name": author})
                return authors_list
        return None

    def _parse_journal(self, doc):
        """ Returns the journal of a document as a <class 'str'>. """
        if 'source_title' in doc.keys():
            if doc['source_title'] != '':
                return doc['source_title']
        return None

    def _parse_issn(self, doc):
        """ Returns the ISSN and (or) EISSN of a document as a <class 'list'> of <class 'str'> """
        return None

    def _parse_journal_short(self, doc):
        """ Returns the shortend journal name of a document as a <class 'str'>, if available.
         e.g. 'Comp. Mat. Sci.' """
        return None

    def _parse_publication_date(self, doc):
        """ Returns the publication_date of a document as a <class 'datetime.datetime'>"""
        if 'publication_date' in doc.keys():
            return datetime.strptime(doc['publication_date'], '%Y-%m-%d')
        elif 'publication_year' in doc.keys():
            return datetime.strptime(str(doc['publication_year']), '%Y')
        return doc['last_updated']

    def _parse_has_year(self, doc):
        """ Returns a <class 'bool'> specifying whether a document's year can be trusted."""
        if 'pubyear' in doc.keys():
            return doc['pubyear'] != None and doc['pubyear'] != ''
        else:
            return self._parse_publication_date(doc) != None

    def _parse_has_month(self, doc):
        """ Returns a <class 'bool'> specifying whether a document's month can be trusted."""
        return self._parse_publication_date(doc) != None

    def _parse_has_day(self, doc):
        """ Returns a <class 'bool'> specifying whether a document's day can be trusted."""
        return self._parse_publication_date(doc) != None

    def _parse_abstract(self, doc):
        """ Returns the abstract of a document as a <class 'str'>"""
        if 'abstract' in doc.keys():
            if doc['abstract'] != '':
                abstract = doc['abstract']
                if '\n' in abstract:
                    abstract = abstract.replace('\n', ' ')
                if '\u2010' in abstract:
                    abstract = abstract.replace('\u2010', '-')
                if 'xmlns' in abstract:
                    return None
                return abstract
        elif 'description' in doc.keys():
            if doc['description'] != '':
                return doc['description']
        return None

    def _parse_origin(self, doc):
        """ Returns the origin of the document as a <class 'str'>. Use the mongodb collection
        name for this."""
        return self.collection_name

    def _parse_source_display(self, doc):
        """ Returns the source of the document as a <class 'str'>. This is what will be
        displayed on the website, so use something people will recognize properly and
        use proper capitalization."""
        if self.collection_name == 'Dimensions_publications':
            return 'Dimensions Publications'
        elif self.collection_name == 'Dimensions_datasets':
            return 'Dimensions Data Sets'
        elif self.collection_name == 'Dimensions_clinical_trials':
            return 'Dimensions Clinical Trials'

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
        if doc['source_linkout'] != '':
            return doc['source_linkout']
        elif doc['dimensions_url'] != '':
            return doc['dimensions_url']
        return None

    def _parse_category_human(self, doc):
        """ Returns the category_human of a document as a <class 'list'> of <class 'str'>"""
        return None

    def _parse_keywords(self, doc):
        """ Returns the keywords for a document from original source as a a <class 'list'> of <class 'str'>"""
        return None

    def _parse_summary_human(self, doc):
        """ Returns the human-written summary of a document as a <class 'list'> of <class 'str'>"""
        return None

    def _parse_is_preprint(self, doc):
        """ Returns a <class 'bool'> specifying whether the document is a preprint.
        If it's not immediately clear from the source it's coming from, return None."""
        return None

    def _parse_is_covid19(self, doc):
        """ Returns a <class 'bool'> if we know for sure a document is specifically about COVID-19.
        If it's not immediately clear from the source it's coming from, return None."""
        return None

    def _parse_license(self, doc):
        """ Returns the license of a document as a <class 'str'> if it is specified in the original doc."""
        return None

    def _parse_pmcid(self, doc):
        """ Returns the pmcid of a document as a <class 'str'>."""
        if 'pmcid' in doc.keys():
            if doc['pmcid'] != '':
                return doc['pmcid']
        return find_remaining_ids(self._parse_doi(doc))['pmcid']

    def _parse_pubmed_id(self, doc):
        """ Returns the PubMed ID of a document as a <class 'str'>."""
        if 'pmid' in doc.keys():
            if doc['pmcid'] != '':
                return doc['pmcid']
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
        if self.collection_name == 'Dimensions_publications':
            return 'paper'
        if self.collection_name == 'Dimensions_datasets':
            return 'dataset'
        if self.collection_name == 'Dimensions_clinical_trials':
            return 'clinical_trial'

    def _preprocess(self, doc):
        """
        Preprocesses an entry from the Elsevier_corona_meta collection into a flattened
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

class UnparsedDimensionsPubDocument(DynamicDocument):
    meta = {"collection": "Dimensions_publications"
    }

    parser = DimensionsParser(collection="Dimensions_publications")

    parsed_class = DimensionsDocument

    parsed_document = ReferenceField(DimensionsDocument, required=False)

    last_updated = DateTimeField(db_field="date_added")

    def parse(self):
        parsed_document = self.parser.parse(self.to_mongo())
        parsed_document['_bt'] = datetime.now()
        parsed_document['unparsed_document'] = self
        return DimensionsDocument(**parsed_document)

class UnparsedDimensionsDataDocument(DynamicDocument):
    meta = {"collection": "Dimensions_datasets"
    }

    parser = DimensionsParser(collection="Dimensions_datasets")

    parsed_class = DimensionsDocument

    parsed_document = ReferenceField(DimensionsDocument, required=False)

    last_updated = DateTimeField(db_field="date_added")

    def parse(self):
        parsed_document = self.parser.parse(self.to_mongo())
        parsed_document['_bt'] = datetime.now()
        parsed_document['unparsed_document'] = self
        return DimensionsDocument(**parsed_document)

class UnparsedDimensionsTrialDocument(DynamicDocument):
    meta = {"collection": "Dimensions_clinical_trials"
    }

    parser = DimensionsParser(collection="Dimensions_clinical_trials")

    parsed_class = DimensionsDocument

    parsed_document = ReferenceField(DimensionsDocument, required=False)

    last_updated = DateTimeField(db_field="date_added")

    def parse(self):
        parsed_document = self.parser.parse(self.to_mongo())
        parsed_document['_bt'] = datetime.now()
        parsed_document['unparsed_document'] = self
        return DimensionsDocument(**parsed_document)
