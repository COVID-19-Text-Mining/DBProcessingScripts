from base import Parser, VespaDocument, indexes
from datetime import datetime
import json
import requests
from utils import clean_title, find_cited_by, find_references, find_remaining_ids
from pprint import PrettyPrinter
import xml.etree.ElementTree as ET
from lxml import etree
from mongoengine import DynamicDocument, ReferenceField, DateTimeField, DictField
import dateutil.parser

latest_version = 5

class LitCovidDocument(VespaDocument):
    meta = {"collection": "Litcovid_parsed_vespa",
            "indexes": indexes
    }

    latest_version = latest_version
    unparsed_document = ReferenceField('UnparsedLitCovidDocument', required=True)

class LitCovidParser(Parser):
    """
    Parser for documents from LitCovid
    """

    def _parse_doi(self, doc):
        """ Returns the DOI of a document as a <class 'str'>"""
        doi_fetch = find_remaining_ids(str(doc['pmid'])).get('doi', None)
        if doi_fetch != None:
            return doi_fetch
        return None


    def _parse_title(self, doc):
        """ Returns the title of a document as a <class 'str'>"""
        return doc['passages'][0]['text']

    def _parse_authors(self, doc):
        """ Returns the authors of a document as a <class 'list'> of <class 'dict'>.
        Each element in the authors list should have a "name" field with the author's
        full name (e.g. John Smith or J. Smith) as a <class 'str'>.
        """
        if 'authors' in doc.keys():
            authors = []
            for author in doc['authors']:
                a = {}
                if ' ' in author:
                    first = '{0}. '.format(author.split(' ', 1)[1])
                    last = author.split(' ', 1)[0]
                    a['first_name'] = first
                    a['last_name'] = last
                else:
                    a['name'] = author
                authors.append(a)
            return authors
        return None

    def _parse_journal(self, doc):
        """ Returns the journal of a document as a <class 'str'>. """
        if '2020' in doc['journal']:
            return doc['journal'].split('2020', 1)[0]
        return doc['journal']

    def _parse_issn(self, doc):
        """ Returns the ISSN and (or) EISSN of a document as a <class 'list'> of <class 'str'> """
        return None

    def _parse_journal_short(self, doc):
        """ Returns the shortend journal name of a document as a <class 'str'>, if available.
         e.g. 'Comp. Mat. Sci.' """
        return None

    def _parse_datestring(self, doc):
        try:
            if '2020' in doc['passages'][0]['infons']['journal']:
                temp = doc['passages'][0]['infons']['journal']
                temp = temp[temp.find('2020'):]
                semicolon = int(temp.find(';'))
                period = int(temp.find('.'))
                colon = int(temp.find(':'))
                pot_boundaries = [semicolon, period, colon]
                boundaries = [boundary for boundary in pot_boundaries if boundary >= 0]
                if boundaries != []:
                    temp = temp[:min(boundaries)]

                if temp.startswith(' '):
                    temp = temp[1:]
                if temp.endswith(' '):
                    temp = temp[:-1]
                datestring = ''
                datelist = temp.split()
                if len(temp) > 3:
                   return datestring.join(datelist[:3])
                else:
                   return datestring.join(datelist)
        except:
            return None

    def _parse_publication_date(self, doc):
        """ Returns the publication_date of a document as a <class 'datetime.datetime'>"""
        datestring = self._parse_datestring(doc)
        if datestring is None:
            return datetime.now()
        try:
            return dateutil.parser.parse(datestring)
        except dateutil.parser._parser.ParserError:
            datestring = datestring.split('-')[0]
            try:
                if len(datestring) > 7:
                    return datetime.strptime(datestring,'%Y%b%d')
                else:
                    return datetime.strptime(datestring[:7], '%Y%b')
            except:
                return datetime.strptime(datestring[:4],"%Y")

    def _parse_has_year(self, doc):
        """ Returns a <class 'bool'> specifying whether a document's year can be trusted."""
        return 'year' in doc['passages'][0]['infons'].keys() and doc['passages'][0]['infons']['year'] != ''

    def _parse_has_month(self, doc):
        """ Returns a <class 'bool'> specifying whether a document's month can be trusted."""
        if self._parse_datestring(doc) != None:
            return len(self._parse_datestring(doc)) >= 6
        else:
            return False

    def _parse_has_day(self, doc):
        """ Returns a <class 'bool'> specifying whether a document's day can be trusted."""
        if self._parse_datestring(doc) != None:
            return len(self._parse_datestring(doc)) >= 8
        else:
            return False

    def _parse_abstract(self, doc):
        """ Returns the abstract of a document as a <class 'str'>"""
        for passage in doc['passages']:
            type = passage['infons']['type']
            if type == 'ABSTRACT' or type == 'abstract':
                if passage['text'] != '':
                    return passage['text']
        return None


    def _parse_origin(self, doc):
        """ Returns the origin of the document as a <class 'str'>. Use the mongodb collection
        name for this."""
        return 'LitCovid2BioCJSON'

    def _parse_source_display(self, doc):
        """ Returns the source of the document as a <class 'str'>. This is what will be
        displayed on the website, so use something people will recognize properly and
        use proper capitalization."""
        return 'PubMed'

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
        if self._parse_doi(doc) != None:
            link = 'https://doi.org/' + self._parse_doi(doc)
        else:
            link = 'https://www.ncbi.nlm.nih.gov/pubmed/{}'.format(str(doc['pmid']))
        return link

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
        return True

    def _parse_license(self, doc):
        """ Returns the license of a document as a <class 'str'> if it is specified in the original doc."""
        return None

    def _parse_cord_uid(self, doc):
        """ Returns the cord_uid of a document as a <class 'str'> if it is available."""
        return None

    def _parse_pmcid(self, doc):
        """ Returns the pmcid of a document as a <class 'str'>."""
        try:
            return doc['pmcid']
        except:
            return find_remaining_ids(str(doc['pmid']))['pmcid']

    def _parse_pubmed_id(self, doc):
        """ Returns the PubMed ID of a document as a <class 'str'>."""
        return str(doc['pmid'])

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
        return 'paper'

    def _preprocess(self, doc):
        """
        Preprocesses an entry from the LitCovid_crossref and LitCovid_pubmed_xml
        collections into a flattened metadata document.
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

class UnparsedLitCovidDocument(DynamicDocument):
    meta = {"collection": "LitCovid2BioCJSON"
    }

    parser = LitCovidParser()

    parsed_class = LitCovidDocument

    parsed_document = ReferenceField(LitCovidDocument, required=False)

    last_updated = DateTimeField(db_field='last_updated')
    
    def parse(self):
        parsed_document = self.parser.parse(self.to_mongo())
        parsed_document['_bt'] = datetime.now()
        parsed_document['unparsed_document'] = self
        return LitCovidDocument(**parsed_document)

