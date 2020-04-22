from base import Parser
import json
import datetime
import requests
from utils import clean_title, find_cited_by, find_references, find_pmcid_and_pubmed_id
from pprint import PrettyPrinter
import xml.etree.ElementTree as ET


class LitCovidParser(Parser):
    """
    Parser for documents from LitCovid
    """

    def _parse_doi(self, doc):
        """ Returns the DOI of a document as a <class 'str'>"""
        if 'xml' in doc.keys():
            doc = ET.fromstring(doc['xml'])
            IDs = doc.find('PubmedArticle').find('PubmedData').find('ArticleIdList')
            if IDs:
                for id in IDs.iter('ArticleId'):
                    if id.attrib['IdType'] == 'doi':
                        return id.text
        elif type(doc) == dict:
            return doc['DOI']
        return None

    def _parse_title(self, doc):
        """ Returns the title of a document as a <class 'str'>"""
        if 'xml' in doc.keys():
            doc = ET.fromstring(doc['xml'])
            article = doc.find('PubmedArticle').find('MedlineCitation').find('Article')
            return article.find('ArticleTitle').text
        elif type(doc) == dict:
            return doc['title']
        return None

    def _parse_authors(self, doc):
        """ Returns the authors of a document as a <class 'list'> of <class 'dict'>.
        Each element in the authors list should have a "name" field with the author's
        full name (e.g. John Smith or J. Smith) as a <class 'str'>.
        """
        if 'xml' in doc.keys():
            doc = ET.fromstring(doc['xml'])
            article = doc.find('PubmedArticle').find('MedlineCitation').find('Article')
            authors = article.find('AuthorList')
            if authors:
                if authors.attrib['CompleteYN'] == 'Y':
                    authors_list = []
                    for author in authors.iter('Author'):
                        if author.attrib['ValidYN'] == 'Y':
                            author_info = dict()
                            if author.find('ForeName') != None and author.find('LastName') != None:
                                first = author.find('ForeName').text
                                last = author.find('LastName').text
                                name = first + last
                                author_info['Name'] = u'{0} {1}'.format(first, last)
                                authors_list.append(author_info)
                    return authors_list
        elif type(doc) == dict:
            if 'author' in doc:
                authors = []
                for author in doc['author']:
                    if 'given' in author:
                        name = '{0} {1}'.format(author['given'], author['family'])
                        first = author['given']
                        last = author['family']
                    else:
                        name = '{0}'.format(author['family'])
                        last = author['family']
                    affiliation = author['affiliation']
                    authors.append({'name' : name})
                return authors
        return None

    def _parse_journal(self, doc):
        """ Returns the journal of a document as a <class 'str'>. """
        if 'xml' in doc.keys():
            doc = ET.fromstring(doc['xml'])
            article = doc.find('PubmedArticle').find('MedlineCitation').find('Article')
            return article.find('Journal').find('Title').text
        elif type(doc) == dict:
            if 'container-title' in doc.keys():
                return doc['container-title']
        return None

    def _parse_issn(self, doc):
        """ Returns the ISSN and (or) EISSN of a document as a <class 'list'> of <class 'str'> """
        if type(doc) == dict:
            if 'ISSN' in doc.keys():
                return doc['ISSN']
        return None

    def _parse_journal_short(self, doc):
        """ Returns the shortend journal name of a document as a <class 'str'>, if available.
         e.g. 'Comp. Mat. Sci.' """
        if type(doc) == dict:
            if 'short-container-title' in doc.keys():
                return doc['short-container-title']
        return None

    def _parse_publication_date(self, doc):
        """ Returns the publication_date of a document as a <class 'datetime.datetime'>"""
        if 'xml' in doc.keys():
            doc = ET.fromstring(doc['xml'])
            article = doc.find('PubmedArticle').find('MedlineCitation').find('Article')
            ArticleDate = article.find('ArticleDate')
            if ArticleDate:
                if ArticleDate.find('Day') != None:
                    day = ArticleDate.find('Day').text
                    month = ArticleDate.find('Month').text
                    if len(month) == 3:
                        month = list(calendar.month_abbr).index(month)
                    year = ArticleDate.find('Year').text
                    datestring = "{0}-{1}-{2}".format(year, month, day)
                    return datetime.datetime.strptime(datestring, '%Y-%m-%d')
                elif ArticleDate.find('Month') != None:
                    month = ArticleDate.find('Month').text
                    if len(month) == 3:
                        month = list(calendar.month_abbr).index(month)
                    year = ArticleDate.find('Year').text
                    datestring = "{0}, {1}".format(year, month)
                    return datetime.datetime.strptime(datestring, '%Y-%m')
                elif ArticleDate.find('Year') != None:
                    year = ArticleDate.find('Year').text
                    datestring = "{0}".format(year)
                    return datetime.datetime.strptime(datestring, '%Y')
        elif type(doc) == dict:
            formatted_date = ""
            date = doc['issued']['date-parts']
            if len(date[0]) == 1 and date[0] != None:
                datestring = "{0}".format(date[0][0])
                return datetime.datetime.strptime(datestring, '%Y')
            elif len(date[0]) == 2:
                datestring = "{0}-{1}".format(date[0][0], date[0][1])
                return datetime.datetime.strptime(datestring, '%Y-%m')
            else:
                datestring = "{0}-{1}-{2}".format(date[0][0], date[0][1], date[0][2])
                return datetime.datetime.strptime(datestring, '%Y-%m-%d')
        return None

    def _parse_has_year(self, doc):
        """ Returns a <class 'bool'> specifying whether a document's year can be trusted."""
        if 'xml' in doc.keys():
            doc = ET.fromstring(doc['xml'])
            article = doc.find('PubmedArticle').find('MedlineCitation').find('Article')
            return article.find('ArticleDate') and article.find('ArticleDate').find('Year').text
        elif type(doc) == dict:
            return 'date-parts' in doc['issued'] and len(doc['issued']['date-parts'][0]) >= 1
        return None

    def _parse_has_month(self, doc):
        """ Returns a <class 'bool'> specifying whether a document's month can be trusted."""
        if 'xml' in doc.keys():
            doc = ET.fromstring(doc['xml'])
            article = doc.find('PubmedArticle').find('MedlineCitation').find('Article')
            return article.find('ArticleDate') and article.find('ArticleDate').find('Month').text
        elif type(doc) == dict:
            return 'date-parts' in doc['issued'] and len(doc['issued']['date-parts'][0]) >= 2
        return None

    def _parse_has_day(self, doc):
        """ Returns a <class 'bool'> specifying whether a document's day can be trusted."""
        if 'xml' in doc.keys():
            doc = ET.fromstring(doc['xml'])
            article = doc.find('PubmedArticle').find('MedlineCitation').find('Article')
            return article.find('ArticleDate') and article.find('ArticleDate').find('Day').text
        elif type(doc) == dict:
            return 'date-parts' in doc['issued'] and len(doc['issued']['date-parts'][0]) == 3
        return None

    def _parse_abstract(self, doc):
        """ Returns the abstract of a document as a <class 'str'>"""
        if 'xml' in doc.keys():
            doc = ET.fromstring(doc['xml'])
            article = doc.find('PubmedArticle').find('MedlineCitation').find('Article')
            if article.find('Abstract') != None:
                abstract_sections = article.find('Abstract')
                abstract = ""
                for section in abstract_sections.iter('AbstractText'):
                    if 'Label' in section.attrib and section.text != None:
                        heading = section.attrib['Label'] + ': '
                        abstract = abstract + heading + section.text + ' '
                    elif section.text != None:
                        abstract = abstract + section.text
                return abstract
            return None
        elif type(doc) == dict:
            if 'abstract' in doc.keys():
                return doc['abstract']
        return None


    def _parse_origin(self, doc):
        """ Returns the origin of the document as a <class 'str'>. Use the mongodb collection
        name for this."""
        if 'xml' in doc.keys():
            doc = ET.fromstring(doc['xml'])
            return "LitCovid_pubmed_xml"
        elif type(doc) == dict:
            return "LitCovid_crossref"

    def _parse_source_display(self, doc):
        """ Returns the source of the document as a <class 'str'>. This is what will be
        displayed on the website, so use something people will recognize properly and
        use proper capitalization."""
        return 'LitCovid'

    def _parse_last_updated(self, doc):
        """ Returns when the entry was last_updated as a <class 'datetime.datetime'>. Note
        this should probably not be the _bt field in a Parser."""
        return datetime.datetime.now()

    def _parse_has_full_text(self, doc):
        """ Returns a <class 'bool'> specifying if we have the full text."""
        pass

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
        if type(doc) == dict and 'link' in doc.keys():
            return doc['link'][0]['URL']
        elif self._parse_doi(doc):
            return 'https://doi.org/' + self._parse_doi(doc)
        return

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
        return False

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
        return find_pmcid_and_pubmed_id(self._parse_doi(doc))['pmcid']

    def _parse_pubmed_id(self, doc):
        """ Returns the PubMed ID of a document as a <class 'str'>."""
        return find_pmcid_and_pubmed_id(self._parse_doi(doc))['pubmed_id']

    def _parse_who_covidence(self, doc):
        """ Returns the who_covidence of a document as a <class 'str'>."""
        return None

    def _parse_version(self, doc):
        """ Returns the version of a document as a <class 'int'>."""
        return 1

    def _parse_copyright(self, doc):
        """ Returns the copyright notice of a document as a <class 'str'>."""
        return None

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