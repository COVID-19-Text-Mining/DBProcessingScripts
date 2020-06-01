from datetime import datetime
import collections

from paper_metadata.metadata_doc import MetadataDocument
from paper_metadata.common_utils import parse_date
from paper_metadata.common_utils import parse_names
from parsers.utils import find_remaining_ids

from parsers.base import Parser

class ScopusParser(Parser):
    """
    Parser for result from scopus API
    """
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.parse_functions = {
            'doi': self._parse_doi,
            'title': self._parse_title,
            'authors': self._parse_authors,
            'journal': self._parse_journal,
            'journal_short': self._parse_journal_short,
            'issn': self._parse_issn,
            'abstract': self._parse_abstract,
            'origin': self._parse_origin,
            'source_display': self._parse_source_display,
            'last_updated': self._parse_last_updated,
            'body_text': self._parse_body_text,
            'has_full_text': self._parse_has_full_text,
            'references': self._parse_references,
            'cited_by': self._parse_cited_by,
            'link': self._parse_link,
            'category_human': self._parse_category_human,
            'keywords': self._parse_keywords,
            'summary_human': self._parse_summary_human,
            'is_preprint': self._parse_is_preprint,
            'is_covid19': self._parse_is_covid19,
            'license': self._parse_license,
            'who_covidence': self._parse_who_covidence,
            'version': self._parse_version,
            'copyright': self._parse_copyright,
            'document_type': self._parse_document_type,

            '_publish_date': self._parse_publish_date,
        }


        self.post_functions = collections.OrderedDict({
            'publication_date': self._postprocess_publication_date,
            'has_year': self._postprocess_has_year,
            'has_month': self._postprocess_has_month,
            'has_day': self._postprocess_has_day,
            'pmcid': self._postprocess_pmcid,
            'pubmed_id': self._postprocess_pubmed_id,
        })

    def _parse_doi(self, doc):
        """ Returns the DOI of a document as a <class 'str'>"""
        # doi
        doi = None

        if (doi is None
            and 'doi' in doc
            and isinstance(doc['doi'], str)
            and len(doc['doi']) > 0
        ):
            doi = doc['doi']
        return doi

    def _parse_title(self, doc):
        """ Returns the title of a document as a <class 'str'>"""
        # title
        title = None

        if (title is None
            and 'title' in doc
            and isinstance(doc['title'], str)
            and len(doc['title']) > 0
        ):
            title = doc['title']

        return title

    def _parse_authors(self, doc):
        """ Returns the authors of a document as a <class 'list'> of <class 'dict'>.
        Each element in the authors list should have a "name" field with the author's
        full name (e.g. John Smith or J. Smith) as a <class 'str'>. Feel free to include
        any other fields you have under other field names. Suggested fields are: "first_name",
        "middle_name", "last_name", "institution", "email".
        """
        # authors
        # TODO:
        ...
        authors = None

        if (authors is None
            and 'author_names' in doc
            and isinstance(doc['author_names'], str)
            and len(doc['author_names']) > 0
        ):
            authors = []
            names_parsed = parse_names(doc['author_names'])
            for x in names_parsed:
                if not ('first' in x and 'last' in x):
                    continue
                authors.append({
                    'first_name': x['first'] if x['first'] else '',
                    'middle_name': '',
                    'last_name': x['last'] if x['last'] else '',
                    'name': '',
                    'institution': '',
                    'email': '',
                })
            if len(authors) == 0:
                authors = None
        return authors

    def _parse_journal(self, doc):
        """ Returns the journal of a document as a <class 'str'>. """
        # journal_name
        journal_name = None

        if (journal_name is None
            and 'publicationName' in doc
            and isinstance(doc['publicationName'], str)
            and len(doc['publicationName']) > 0
        ):
            journal_name = doc['publicationName']

        return journal_name

    def _parse_journal_short(self, doc):
        """ Returns the shortend journal name of a document as a <class 'str'>, if available.
         e.g. 'Comp. Mat. Sci.' """
        return None

    def _parse_issn(self, doc):
        """ Returns the ISSN and (or) EISSN for the journal of a document as a
        <class 'str'> """
        ISSN = None
        # ISSN
        if (ISSN is None
            and 'issn' in doc
            and isinstance(doc['issn'], str)
            and len(doc['issn']) > 0
        ):
            ISSN = doc['issn']
        return ISSN

    def _parse_publish_date(self, doc):
        # publish_date
        publish_date = None
        if (publish_date is None
            and 'coverDate' in doc
            and isinstance(doc['coverDate'], str)
            and len(doc['coverDate']) > 0
        ):
            publish_date = parse_date(doc['coverDate'])
        return publish_date


    def _parse_abstract(self, doc):
        """ Returns the abstract of a document as a <class 'str'>"""
        # abstract
        abstract = None

        if (abstract is None
            and 'description' in doc
            and isinstance(doc['description'], str)
            and len(doc['description']) > 0
        ):
            abstract = doc['description']
        return abstract

    def _parse_origin(self, doc):
        """ Returns the origin of the document as a <class 'str'>. Use the mongodb collection
        name for this."""
        return 'Scopus'

    def _parse_source_display(self, doc):
        """ Returns the source of the document as a <class 'str'>. This is what will be
        displayed on the website, so use something people will recognize properly and
        use proper capitalization."""
        return 'Scopus'

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
        This is a list of documents cited by the current document. Try to include "doi"
        as a field for each reference if at all possible.
        """
        # reference
        return None

    def _parse_cited_by(self, doc):
        """ Returns the citations of a document as a <class 'list'> of <class 'dict'>.
        A list of documents that cite this document. Try to include "doi"
        as a field for each citation if at all possible.
        """
        return None

    def _parse_link(self, doc):
        """ Returns the url of a document as a <class 'str'>"""
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

    def _parse_who_covidence(self, doc):
        """ Returns the who_covidence of a document as a <class 'str'>."""
        return None

    def _parse_version(self, doc):
        """ Returns the version of a document as a <class 'int'>."""
        return 1

    def _parse_copyright(self, doc):
        """ Returns the copyright notice of a document as a <class 'str'>."""
        return None

    def _parse_document_type(self, doc):
        """ Returns the document type of a document as a <class 'str'>.
        e.g. 'paper', 'clinical_trial', 'patent', 'news'. """
        return 'metadata'


    def _parse_publication_date(self, doc):
        """ Returns the publication_date of a document as a <class 'datetime.datetime'>"""
        # publish_date
        return NotImplementedError

    def _parse_has_year(self, doc):
        """ Returns a <class 'bool'> specifying whether a document's year can be trusted."""
        return NotImplementedError

    def _parse_has_month(self, doc):
        """ Returns a <class 'bool'> specifying whether a document's month can be trusted."""
        return NotImplementedError

    def _parse_has_day(self, doc):
        """ Returns a <class 'bool'> specifying whether a document's day can be trusted."""
        return NotImplementedError

    def _parse_pmcid(self, doc):
        """ Returns the pmcid of a document as a <class 'str'>."""
        return NotImplementedError

    def _parse_pubmed_id(self, doc):
        """ Returns the PubMed ID of a document as a <class 'str'>."""
        pubmed_id = None

        if (pubmed_id is None
            and 'pubmed_id' in doc
            and isinstance(doc['pubmed_id'], str)
            and len(doc['pubmed_id']) > 0
        ):
            pubmed_id = doc['pubmed_id']
        return pubmed_id

    ###########################################
    # Post - process functions
    ###########################################
    def _postprocess_publication_date(self, doc, parsed_doc):
        """ Returns the publication_date of a document as a <class 'datetime.datetime'>"""
        # publish_date
        date = None
        _publish_date = parsed_doc.get('_publish_date')
        if _publish_date and _publish_date['year']:
            date = datetime(
                year=_publish_date['year'],
                month=_publish_date.get('month', 1),
                day=_publish_date.get('day', 1),
            )
        return date

    def _postprocess_has_year(self, doc, parsed_doc):
        """ Returns a <class 'bool'> specifying whether a document's year can be trusted."""
        result = None
        _publish_date = parsed_doc.get('_publish_date')
        if _publish_date and _publish_date['year']:
            result = True
        return result

    def _postprocess_has_month(self, doc, parsed_doc):
        """ Returns a <class 'bool'> specifying whether a document's month can be trusted."""
        result = None
        _publish_date = parsed_doc.get('_publish_date')
        if _publish_date and _publish_date['month']:
            result = True
        return result

    def _postprocess_has_day(self, doc, parsed_doc):
        """ Returns a <class 'bool'> specifying whether a document's day can be trusted."""
        result = None
        _publish_date = parsed_doc.get('_publish_date')
        if _publish_date and _publish_date['day']:
            result = True
        return result

    def _postprocess_pmcid(self, doc, parsed_doc):
        """ Returns the pmcid of a document as a <class 'str'>."""
        result = None
        if parsed_doc.get('doi'):
            ids = find_remaining_ids(parsed_doc['doi'])
            if ids.get('pmcid'):
                result = ids['pmcid']
        return result

    def _postprocess_pubmed_id(self, doc, parsed_doc):
        """ Returns the PubMed ID of a document as a <class 'str'>."""
        result = None
        if (not parsed_doc.get('pubmed_id')) and parsed_doc.get('doi'):
            ids = find_remaining_ids(parsed_doc['doi'])
            if ids.get('pubmed_id'):
                result = ids['pubmed_id']
        return result

    def _postprocess(self, doc, parsed_doc):
        """
        Post-process an entry to add any last-minute fields required.

        """
        for key, post_func in self.post_functions.items():
            result = post_func(doc, parsed_doc)
            if result is not None:
                parsed_doc[key] = result

        if '_publish_date' in parsed_doc:
            del parsed_doc['_publish_date']

        return parsed_doc

    def parse(self, doc):
        """
        entrance function to parse the crossref api

        :param doc: (dict) doc returned by crossref api
        :return: (dict) metadata of paper
        """
        doc_parsed = {}

        doc = self._preprocess(doc)

        for key, parse_func in self.parse_functions.items():
            result = parse_func(doc)
            if result is not None:
                doc_parsed[key] = result

        doc_parsed = self._postprocess(doc, doc_parsed)

        return doc_parsed

    def get_parsed_doc(self, doc):
        """
        check doc type in parsed doc and return a MetadataDocument object

        :param doc: (dict) doc returned by crossref api
        :return: (object or None) MetadataDocument object with auto type check.
                    If not useful information parsed, return None
        """
        doc_parsed = None
        data_parsed = self.parse(doc)

        data_parsed['_bt'] = datetime.now()

        if len(data_parsed) > 0:
            doc_parsed = MetadataDocument(**data_parsed)
        return doc_parsed


