from base import Parser, VespaDocument, indexes
import os
import pymongo
from datetime import datetime
from utils import clean_title, find_cited_by, find_references
import gridfs
import traceback
from io import BytesIO
from pdf_extractor.paragraphs import extract_paragraphs_pdf
from mongoengine import DynamicDocument, ReferenceField, DateTimeField

latest_version = 3

class BiorxivDocument(VespaDocument):
    meta = {"collection": "biorxiv_parsed_vespa",
            "indexes": indexes
    }

    latest_version = latest_version
    unparsed_document = ReferenceField('UnparsedBiorxivDocument', required=True)

class BiorxivParser(Parser):

    def __init__(self, parse_full_text=False):
        """
        Parser for documents scraped from the BioRxiv/medRxiv preprint servers.
        """

        self.parse_full_text = parse_full_text

        client = pymongo.MongoClient(os.getenv("COVID_HOST"), username=os.getenv("COVID_USER"),
                                     password=os.getenv("COVID_PASS"), authSource=os.getenv("COVID_DB"))

        self.db = client[os.getenv("COVID_DB")]

    def _parse_doi(self, doc):
        """ Returns the DOI of a document as a <class 'str'>"""
        return doc.get('Doi', None)

    def _parse_title(self, doc):
        """ Returns the title of a document as a <class 'str'>"""
        title = doc.get("Title", '')
        return clean_title(title)

    def _parse_authors(self, doc):
        """ Returns the authors of a document as a <class 'list'> of <class 'dict'>.
        Each element in the authors list should have a "name" field with the author's
        full name (e.g. John Smith or J. Smith) as a <class 'str'>.
        """
        author_list = doc.get("Authors", [])
        authors = []
        for a in author_list:
            author = {}
            author['name'] = a['Name']['fn'] + " " + a['Name']['ln']
            author['first_name'] = a['Name']['fn']
            author['last_name'] = a['Name']['ln']
            authors.append(author)
        return authors

    def _parse_journal(self, doc):
        """ Returns the journal of a document as a <class 'str'>. """
        return doc.get('Journal', None)

    def _parse_issn(self, doc):
        """ Returns the ISSN and (or) EISSN of a document as a <class 'list'> of <class 'str'> """
        return None

    def _parse_journal_short(self, doc):
        """ Returns the shortend journal name of a document as a <class 'str'>, if available.
         e.g. 'Comp. Mat. Sci.' """
        return None

    def _parse_publication_date(self, doc):
        """ Returns the publication_date of a document as a <class 'datetime.datetime'>"""
        return doc.get("Publication_Date", doc.get('last_updated', datetime.now()))

    def _parse_has_year(self, doc):
        """ Returns a <class 'bool'> specifying whether a document's year can be trusted."""
        return True

    def _parse_has_month(self, doc):
        """ Returns a <class 'bool'> specifying whether a document's month can be trusted."""
        return True

    def _parse_has_day(self, doc):
        """ Returns a <class 'bool'> specifying whether a document's day can be trusted."""
        return True

    def _parse_abstract(self, doc):
        """ Returns the abstract of a document as a <class 'str'>"""
        return ' '.join(doc.get('Abstract', []))

    def _parse_origin(self, doc):
        """ Returns the origin of the document as a <class 'str'>. Use the mongodb collection
        name for this."""
        return "Scraper_connect_biorxiv_org"

    def _parse_source_display(self, doc):
        """ Returns the source of the document as a <class 'str'>. This is what will be
        displayed on the website, so use something people will recognize properly and
        use proper capitalization."""
        return doc['Journal']

    def _parse_last_updated(self, doc):
        """ Returns when the entry was last_updated as a <class 'datetime.datetime'>. Note
        this should probably not be the _bt field in a Parser."""
        return datetime.now()

    def _parse_has_full_text(self, doc):
        """ Returns a <class 'bool'> specifying if we have the full text."""
        return True

    def _parse_body_text(self, doc):
        """ Returns the body_text of a document as a <class 'list'> of <class 'dict'>.
        This should be a list of objects of some kind. Seems to be usually something like
        {'section_heading':  <class 'str'>,
         'text': <class 'str'>
         }

         """
        body_text = None

        if self.parse_full_text:
            paper_fs = gridfs.GridFS(self.db, collection='Scraper_connect_biorxiv_org_fs')
            pdf_file = paper_fs.get(doc['PDF_gridfs_id'])

            try:
                paragraphs = extract_paragraphs_pdf(BytesIO(pdf_file.read()))
            except Exception as e:
                print('Failed to extract PDF %s(%r) (%r)' % (doc['Doi'], doc['PDF_gridfs_id'], e))
                traceback.print_exc()
                paragraphs = []

            body_text = [{
                'section_heading': None,
                'text': x
            } for x in paragraphs]

            return body_text

        return body_text

    def _parse_references(self, doc):
        """ Returns the references of a document as a <class 'list'> of <class 'dict'>.
        This is a list of documents cited by the current document.
        """
        doi = self._parse_doi(doc)
        #TODO: Get these from the article rather than this API
        return find_references(doi)

    def _parse_cited_by(self, doc):
        """ Returns the citations of a document as a <class 'list'> of <class 'str'>.
        A list of DOIs of documents that cite this document.
        """
        doi = self._parse_doi(doc)
        return find_cited_by(doi)

    def _parse_link(self, doc):
        """ Returns the url of a document as a <class 'str'>"""
        url = doc.get("Link", None)
        doi = doc.get("Doi", None)
        if url is None and doi:
            url = 'https://doi.org/' + doi
        return url

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
        return True

    def _parse_is_covid19(self, doc):
        """ Returns a <class 'bool'> if we know for sure a document is specifically about COVID-19.
        If it's not immediately clear from the source it's coming from, return None."""
        return True

    def _parse_license(self, doc):
        """ Returns the license of a document as a <class 'str'> if it is specified in the original doc."""
        #TODO: Get this from the scraper
        return 'biorxiv'

    def _parse_pmcid(self, doc):
        """ Returns the pmcid of a document as a <class 'str'>."""
        return None

    def _parse_pubmed_id(self, doc):
        """ Returns the PubMed ID of a document as a <class 'str'>."""
        return None

    def _parse_who_covidence(self, doc):
        """ Returns the who_covidence of a document as a <class 'str'>."""
        return None

    def _parse_version(self, doc):
        """ Returns the version of a document as a <class 'int'>."""
        return latest_version

    def _parse_copyright(self, doc):
        """ Returns the copyright notice of a document as a <class 'str'>."""
        # TODO: Get this from the scraper
        None

    def _parse_document_type(self, doc):
        """ Returns the document type of a document as a <class 'str'>.
        e.g. 'paper', 'clinical_trial', 'patent', 'news'. """
        return 'paper'

    def _postprocess(self, doc, parsed_doc):
        """
        Post-process an entry to add any last-minute fields required. Gets called right before parsed doc is
        returned. Return parsed_doc if you don't want to make any changes.
`
        """
        # Apparently the builder needs this to be happy.
        parsed_doc['PDF_gridfs_id'] = doc.get('PDF_gridfs_id', None)
        return parsed_doc

class UnparsedBiorxivDocument(DynamicDocument):
    meta = {"collection": "Scraper_connect_biorxiv_org"
    }

    parser = BiorxivParser()

    parsed_class = BiorxivDocument

    parsed_document = ReferenceField(BiorxivDocument, required=False)

    last_updated = DateTimeField(db_field="last_updated")

    def parse(self):
        parsed_document = self.parser.parse(self.to_mongo())
        parsed_document['_bt'] = datetime.now()
        parsed_document['unparsed_document'] = self
        del(parsed_document['PDF_gridfs_id'])
        return BiorxivDocument(**parsed_document)