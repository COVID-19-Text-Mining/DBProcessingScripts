from base import Parser, VespaDocument, indexes
import json
import re
from datetime import datetime
import requests
from utils import clean_title, clean_abstract, find_cited_by, find_references
from mongoengine import DynamicDocument, ReferenceField, DateTimeField, GenericReferenceField
from collections import defaultdict

latest_version = 2

class CORD19Document(VespaDocument):
    meta = {"collection": "CORD_parsed_vespa",
            "indexes": indexes
            }

    latest_version = latest_version
    unparsed_document = GenericReferenceField(required=True)


class CORD19Parser(Parser):
    def __init__(self, collection):
        """
        Parser for documents from the CORD-19 dataset from Semantic Scholar.
        """

        self.collection_name = collection

    def _parse_doi(self, doc):
        """ Returns the DOI of a document as a <class 'str'>"""
        doi = doc.get('doi', None)
        if doi:
            doi = doi.strip()
        return doi

    def _parse_title(self, doc):
        """ Returns the title of a document as a <class 'str'>"""
        title = doc["metadata"].get("title", None)
        return clean_title(title)

    def _parse_authors(self, doc):
        """ Returns the authors of a document as a <class 'list'> of <class 'dict'>.
        Each element in the authors list should have a "name" field with the author's
        full name (e.g. John Smith or J. Smith) as a <class 'str'>.
        """

        author_list = []
        if "crossref_raw_result" in doc:
            for a in doc['metadata']['authors']:
                author = dict()
                name = ""
                if a['first'] and a['first'] != "":
                    name += a['first']
                if len(a['middle']) > 0:
                    name += " " + " ".join([m for m in a['middle']])
                if a['last'] and a['last'] != "":
                    name += " " + a['last']
                if a['suffix'] and a['suffix'] != "":
                    name += " " + a['suffix']
                author['name'] = name

                if a['email'] != "":
                    author['email'] = a['email'].strip()
                    if author['email'][-1] == ".":
                        author['email'] = author['email'][:-1]

                if 'institution' in a['affiliation'].keys():
                    author['institution'] = a['affiliation']['institution']

                if len(author['name']) > 3:
                    author_list.append(author)
        elif 'csv_raw_result' in doc:
            author_list = [{'name': a} for a in doc['csv_raw_result']['authors'].split(';')]
        else:
            author_list = []
            
        return author_list

    def _parse_journal(self, doc):
        """ Returns the journal of a document as a <class 'str'>. """
        journal = doc.get('journal_name', None)
        if not journal:
            journal_maybe_array = doc.get('crossref_raw_result', {}).get("container-title", None)
            if journal is not None:
                journal = journal_maybe_array[0] if isinstance(journal_maybe_array, list) else journal_maybe_array
            else:
                journal = doc.get('crossref_raw_result', {}).get('institution', {}).get("name", None)

        return journal

    def _parse_issn(self, doc):
        """ Returns the ISSN and (or) EISSN of a document as a <class 'list'> of <class 'str'> """
        issn = doc.get('ISSN', None)
        if issn is None:
            issn_maybe_array = doc.get("crossref_raw_result", {}).get("ISSN", None)
            issn = issn_maybe_array[0] if isinstance(issn_maybe_array, list) else issn_maybe_array
        if issn:
            issn = issn.replace("-", "")
        return issn

    def _parse_journal_short(self, doc):
        """ Returns the shortend journal name of a document as a <class 'str'>, if available.
         e.g. 'Comp. Mat. Sci.' """
        journal_maybe_array = doc.get('crossref_raw_result', {}).get("short-container-title")
        journal = journal_maybe_array[0] if isinstance(journal_maybe_array, list) and len(journal_maybe_array) > 0 else str(journal_maybe_array)
        return journal

    def parse_date_parts(self, doc):
        """ Parses date and whether the various parts of the date can be trusted."""
        pd = doc.get("publish_date", None)
        if pd is None:
            pd = doc.get('crossref_raw_result', {}).get('published-print', {}).get('date-parts', None)
            if pd:
                pd = pd[0]

        publication_date_dict = defaultdict(lambda: 1)
        parsed_date = {}
        if isinstance(pd, dict):
            for k, v in pd.items():
                if v is not None:
                    publication_date_dict[k] = v

            if 'year' in pd.keys() and pd['year'] is not None:
                # Year is mandatory
                parsed_date['publication_date'] = datetime(year=pd['year'],
                                                           month=publication_date_dict['month'],
                                                           day=publication_date_dict['day'])
                parsed_date['has_year'] = True

                parsed_date['has_month'] = 'month' in publication_date_dict.keys()
                parsed_date['has_day'] = 'day' in publication_date_dict.keys()

            else:
                parsed_date['publication_date'] = None
                parsed_date['has_year'] = False
                parsed_date['has_month'] = False
                parsed_date['has_day'] = False

        elif isinstance(pd, list):
            if len(pd) == 2 and all([x is not None for x in pd]):
                parsed_date['publication_date'] = datetime(year=pd[0], month=pd[1], day=1)

                parsed_date['has_year'] = True
                parsed_date['has_month'] = True
                parsed_date['has_day'] = False

            elif len(pd) >= 1 and pd[0] is not None:
                parsed_date['publication_date'] = datetime(year=pd[0], month=1, day=1)
                parsed_date['has_year'] = True
                parsed_date['has_month'] = False
                parsed_date['has_day'] = False

        else:
            parsed_date["publication_date"] = None
            parsed_date['has_year'] = False
            parsed_date['has_month'] = False
            parsed_date['has_day'] = False

        return parsed_date

    def _parse_publication_date(self, doc):
        """ Returns the publication_date of a document as a <class 'datetime.datetime'>"""

        if "crossref_raw_result" in doc:
            return self.parse_date_parts(doc).get("publication_date", doc['last_updated'])
        elif "csv_raw_result" in doc:
            try:
                date = datetime.strptime(doc['csv_raw_result']['publish_time'],
                                                  '%Y-%m-%d')
            except ValueError:
                try:
                    date = datetime.strptime(doc['csv_raw_result']['publish_time'],
                                                      '%Y %b %d')
                except ValueError:
                    try:
                        date = datetime.strptime(
                            doc['csv_raw_result']['publish_time'], '%Y %b')
                    except ValueError:
                        try:
                            date = datetime.strptime(
                                doc['csv_raw_result']['publish_time'], '%Y')
                        except:
                            date = doc['last_updated']
        else:
            date = doc['last_updated']
        return date

    def _parse_has_year(self, doc):
        """ Returns a <class 'bool'> specifying whether a document's year can be trusted."""
        return self.parse_date_parts(doc).get("has_year", False)

    def _parse_has_month(self, doc):
        """ Returns a <class 'bool'> specifying whether a document's month can be trusted."""
        return self.parse_date_parts(doc).get("has_month", False)

    def _parse_has_day(self, doc):
        """ Returns a <class 'bool'> specifying whether a document's day can be trusted."""
        return self.parse_date_parts(doc).get("has_day", False)

    def _parse_abstract(self, doc):
        """ Returns the abstract of a document as a <class 'str'>.
        Prefers to use abstract from crossref if that is available
        """
        if "crossref_raw_result" in doc:

            if 'abstract' in doc['crossref_raw_result'].keys() and len(doc['crossref_raw_result']['abstract']) > 0:
                abstract = doc['crossref_raw_result']['abstract']
            else:
                abstract = ""
                for t in doc['abstract']:
                    abstract += t['text']
        elif 'csv_raw_result' in doc:
            abstract = doc['csv_raw_result']['abstract']
        else:
            abstract = ""

        return clean_abstract(abstract)

    def _parse_origin(self, doc):
        """ Returns the origin of the document as a <class 'str'>. Use the mongodb collection
        name for this."""
        return self.collection_name

    def _parse_source_display(self, doc):
        """ Returns the source of the document as a <class 'str'>. This is what will be
        displayed on the website, so use something people will recognize properly and
        use proper capitalization."""
        return 'CORD-19'

    def _parse_last_updated(self, doc):
        """ Returns when the entry was last_updated as a <class 'datetime.datetime'>. Note
        this should probably not be the _bt field in a Parser."""
        return datetime.now()

    def _parse_has_full_text(self, doc):
        """ Returns a <class 'bool'> specifying if we have the full text."""
        # TODO: Make this method smarter. Lots of documents have "body text" that doesn't really warrant that label.
        return len(doc.get("body_text", [])) > 0

    def _parse_body_text(self, doc):
        """ Returns the body_text of a document as a <class 'list'> of <class 'dict'>.
        This should be a list of objects of some kind. Seems to be usually something like
        {'section_heading':  <class 'str'>,
         'text': <class 'str'>
         }

         """

        body_text = doc.get("body_text", [])
        sections = dict()
        for t in body_text:
            try:
                sections[t['section']] = sections[t['section']] + t['text']
            except KeyError:
                sections[t['section']] = t['text']
        sections_list = [{"section_heading": k, "text": v} for k, v in sections.items()]
        return sections_list

    def _parse_references(self, doc):
        """ Returns the references of a document as a <class 'list'> of <class 'dict'>.
        This is a list of documents cited by the current document.
        """
        bib_entries = []
        if ('bib_entries' in doc
                and isinstance(doc['bib_entries'], dict)
                and len(doc['bib_entries']) > 0
        ):
            for bib in doc['bib_entries'].values():
                bib_entries.append({
                    'ref_id': bib.get('ref_id', ''),
                    'title': bib.get('title', ''),
                    'year': bib.get('year', None),
                    'issn': str(bib.get('issn', '')),
                    'doi': str(bib.get('other_ids', {}).get('DOI', "")),
                    "text": str(bib.get('other_ids', {}).get('DOI', ""))
                })
        if len(bib_entries) == 0:
            doi = self._parse_doi(doc)
            bib_entries = find_references(doi)
        return bib_entries

    def _parse_cited_by(self, doc):
        """ Returns the citations of a document as a <class 'list'> of <class 'str'>.
        A list of DOIs of documents that cite this document.
        """
        doi = self._parse_doi(doc)
        return find_cited_by(doi)

    def _parse_link(self, doc):
        """ Returns the url of a document as a <class 'str'>"""
        doi = self._parse_doi(doc)
        if doi:
            return "https://doi.org/%s" % doi
        return "https://pages.semanticscholar.org/coronavirus-research"

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

        source = doc.get("csv_raw_result", {}).get("source_x", "None")
        if source:
            if source.lower() in ["biorxiv", "medrxiv"]:
                return True
            elif source.lower() in ["pmc", "elsevier", "czi"]:
                return False
        return None

    def _parse_is_covid19(self, doc):
        """ Returns a <class 'bool'> if we know for sure a document is specifically about COVID-19.
        If it's not immediately clear from the source it's coming from, return None."""
        return None

    def _parse_license(self, doc):
        """ Returns the license of a document as a <class 'str'> if it is specified in the original doc."""
        return doc.get("csv_raw_result", {}).get("license", None)

    def _parse_pmcid(self, doc):
        """ Returns the pmcid of a document as a <class 'str'>."""
        pmcid = doc.get("csv_raw_result", {}).get("pmcid", None)
        return pmcid if pmcid != "" else None

    def _parse_pubmed_id(self, doc):
        """ Returns the PubMed ID of a document as a <class 'str'>."""
        pubmed_id = doc.get("csv_raw_result", {}).get("pubmed_id", None)
        return str(pubmed_id) if pubmed_id != "" else None

    def _parse_who_covidence(self, doc):
        """ Returns the who_covidence of a document as a <class 'str'>."""
        who = doc.get("csv_raw_result", {}).get("WHO #Covidence", None)
        return who if who != "" else None

    def _parse_version(self, doc):
        """ Returns the version of a document as a <class 'int'>."""
        return latest_version

    def _parse_document_type(self, doc):
        """ Returns the document type of a document as a <class 'str'>.
        e.g. 'paper', 'clinical_trial', 'patent', 'news'. """
        return 'paper'

    def _parse_copyright(self, doc):
        """ Returns the copyright notice of a document as a <class 'str'>."""
        assertions = doc.get("crossref_raw_result", {}).get("assertion", [])
        for assertion in assertions:
            if assertion.get("name", None) == "copyright":
                return assertion.get("value", None)

    def _parse_cord_uid(self, doc):
        """ Returns the CORD UID of a document as a <class 'str'>."""
        if 'csv_raw_result' in doc.keys() and 'cord_uid' in doc["csv_raw_result"].keys():
            return doc["csv_raw_result"]["cord_uid"]
        else:
            return None




class UnparsedCORD19CustomDocument(DynamicDocument):
    meta = {"collection": "CORD_custom_license"
    }

    parser = CORD19Parser(collection="CORD_custom_license")

    parsed_class = CORD19Document

    parsed_document = ReferenceField(CORD19Document, required=False)

    last_updated = DateTimeField(db_field="last_updated")

    def parse(self):
        parsed_document = self.parser.parse(self.to_mongo())
        parsed_document['_bt'] = datetime.now()
        parsed_document['unparsed_document'] = self
        return CORD19Document(**parsed_document)

class UnparsedCORD19CommDocument(DynamicDocument):
    meta = {"collection": "CORD_comm_use_subset"
    }

    parser = CORD19Parser(collection="CORD_comm_use_subset")

    parsed_class = CORD19Document

    parsed_document = ReferenceField(CORD19Document, required=False)

    last_updated = DateTimeField(db_field="last_updated")

    def parse(self):
        parsed_document = self.parser.parse(self.to_mongo())
        parsed_document['_bt'] = datetime.now()
        parsed_document['unparsed_document'] = self
        return CORD19Document(**parsed_document)

class UnparsedCORD19NoncommDocument(DynamicDocument):
    meta = {"collection": "CORD_noncomm_use_subset"
    }

    parser = CORD19Parser(collection="CORD_noncomm_use_subset")

    parsed_class = CORD19Document

    parsed_document = ReferenceField(CORD19Document, required=False)

    last_updated = DateTimeField(db_field="last_updated")

    def parse(self):
        parsed_document = self.parser.parse(self.to_mongo())
        parsed_document['_bt'] = datetime.now()
        parsed_document['unparsed_document'] = self
        return CORD19Document(**parsed_document)

class UnparsedCORD19XrxivDocument(DynamicDocument):
    meta = {"collection": "CORD_biorxiv_medrxiv"
    }

    parser = CORD19Parser(collection="CORD_biorxiv_medrxiv")

    parsed_class = CORD19Document

    parsed_document = ReferenceField(CORD19Document, required=False)

    last_updated = DateTimeField(db_field="last_updated")

    def parse(self):
        parsed_document = self.parser.parse(self.to_mongo())
        parsed_document['_bt'] = datetime.now()
        parsed_document['unparsed_document'] = self
        return CORD19Document(**parsed_document)
