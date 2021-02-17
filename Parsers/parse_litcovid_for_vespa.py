import pymongo
import os
import datetime
from pprint import pprint
from collections  import defaultdict
import datetime
import requests
import csv
import difflib
import time
import xml.etree.ElementTree as ET
import json
import xmltodict
import calendar

#client = pymongo.MongoClient(os.getenv("COVID_HOST"), username=os.getenv("COVID_USER"),
#                             password=os.getenv("COVID_PASS"), authSource=os.getenv("COVID_DB"))

#db = client[os.getenv("COVID_DB")]


def get_LitCovid_Data(): # Collect most recent data file directly from LitCovid website
    titles_pmids_journals = []
    session = requests.Session()
    LitCovidData_url = 'https://www.ncbi.nlm.nih.gov/research/coronavirus-api/export/tsv?'
    response = session.get(LitCovidData_url)
    data = response.content.decode('utf-8')
    data = data.split('\n')
    data = data[32:]
    for row in data:
        if row != '':
            row = row.split('\t')
            article = dict()
            article['pmid'] = row[0]
            article['title'] = row[1]
            article['journal'] = row[2]
            titles_pmids_journals.append(article)
    return titles_pmids_journals

def pmid2doi(pmid): # Get doi with given pmid (returns None if id cannot be converted)
    session = requests.Session()
    pmid2doi_url = 'https://www.ncbi.nlm.nih.gov/pmc/utils/idconv/v1.0/?ids=%s' % pmid
    response = session.get(pmid2doi_url)
    root = ET.fromstring(response.content)
    for record in root.iter('record'):
        if 'doi' in root.find('record').attrib:
            doi = root.find('record').attrib['doi']
            return doi
        else:
            return

def clean_title(title): # Clean input title for title2doi conversion
    if title.startswith('['):
        title = title[1:-1]
    if title.endswith('.'):
        title = title[0:-1]
    return title

def title2doi(title): # Collect doi for article based on title if doi is not available
    session = requests.Session()
    title = clean_title(title)
    crossref_url = 'https://api.crossref.org/works?query=%s' % title.replace(' ', '%20')
    try:
        response = session.get(crossref_url)
    except:
        print('request to cross_ref failed!')
        return
    try:
        response = response.json()
        cr_response = response['message']
        items = cr_response['items']

        # Title
        top_title = items[0]['title'][0]
        if top_title.endswith(' (Preprint)'):
            top_title = top_title.replace(' (Preprint)', '')
        if u"\u2013" in top_title:
            top_title = top_title.replace(u"\u2013", '-')
        if u"\u2019" in top_title:
            top_title = top_title.replace(u"\u2019", "'")

        # check if top query matches the given title... if it does, return DOI, if it doesn't, return None
        if top_title == title:
            return items[0]['DOI']
        else:
            return
    except:
        print("Couldn't find title on crossref")

def crossref_get(doi): # Scrape raw metadata (json format) from crossref if EFetch fails
    session = requests.Session()
    crossref_url = 'https://api.crossref.org/works/%s' % doi
    try:
        response = session.get(crossref_url)
    except:
        print('request to cross_ref failed!')
        return
    try:
        response = response.json()
        cr_response = response['message']
        return cr_response
    except:
        return

def pubmed_get(pmid): # Scrape raw metadata (xml format)  from pubmed with EFetch
    session = requests.Session()
    pubmed_url = 'https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi?db=pubmed&id=%s&retmode=xml' % pmid
    try:
        response = session.get(pubmed_url)
    except:
        print('Request to PubMed failed!')
        return
    try:
        root = ET.fromstring(response.content)
        article = root.find('PubmedArticle').find('MedlineCitation').find('Article')
        return response
    except:
        return

def crossref_parse(response, pmid, journal): # Parse raw metadata from crossref result for Vespa

    cr_metadata = dict()

    #title
    cr_metadata['title'] = response['title']

    #source
    cr_metadata['source'] = 'PubMed'

    #license
    cr_metadata['license'] = None

    #datestring
    formatted_date = ""
    date = response['issued']['date-parts']
    if len(date[0]) == 1 and date[0] != None:
        formatted_date = "{0}".format(date[0][0])
        cr_metadata['datestring'] = datetime.datetime.strptime(formatted_date, "%Y")
    elif len(date[0]) == 2:
        formatted_date = "{0}-{1}".format(date[0][0], date[0][1])
        cr_metadata['datestring'] = datetime.datetime.strptime(formatted_date, "%Y-%m")
    else:
        formatted_date = "{0}-{1}-{2}".format(date[0][0], date[0][1], date[0][2])
        cr_metadata['datestring'] = datetime.datetime.strptime(formatted_date, "%Y-%m-%d")

    #doi, pmcid, and pubmed_id
    cr_metadata['doi'] = response['DOI']
    cr_metadata['pmcid'] = None
    cr_metadata['pubmed_id'] = pmid

    #url
    if 'link' in response:
        cr_metadata['link'] = response['link']
    else:
        cr_metadata['link'] = 'https://doi.org/' + response['DOI']

    #cord_uid
    cr_metadata['cord_uid'] = None

    #authors
    if 'author' in response:
        authors = []
        for author in response['author']:
            if 'given' in author:
                name = '{0} {1}'.format(author['given'], author['family'])
                first = author['given']
                last = author['family']
            else:
                name = '{0}'.format(author['family'])
                last = author['family']
            affiliation = author['affiliation']
            authors.append({'name' : name})

        cr_metadata['authors'] = authors

    #bib_entries
    if 'reference' in response:
        bibs = []
        references = response['reference']
        i = 0
        for citation in references:
            bib = dict()
            bib['ref_id'] = 'b{0}'.format(i)
            if 'article-title' in citation.keys():
                bib['title'] = citation['article-title']
            if 'year' in citation.keys():
                bib['year'] = citation['year']
            if 'ISSN' in citation.keys():
                bib['issn'] = citation['ISSN']

    #abstract
    if 'abstract' in response:
        cr_metadata['abstract'] = response['abstract']

    #journal
    if journal != None:
        cr_metadata['journal'] = journal
    elif 'container-title' in response.keys():
        cr_metadata['journal'] = response['container-title']
    elif 'short-container-title' in response.keys():
        cr_metadata['journal'] = response['short-container-title']

    #body_text
    cr_metadata['body_text'] = None

    #conclusion
    cr_metadata['conclusion'] = None

    #introduction
    cr_metadata['introduction'] = None

    #results
    cr_metadata['results'] = None

    #discussion
    cr_metadata['discussion'] = None

    #methods
    cr_metadata['methods'] = None

    #background
    cr_metadata['background'] = None

    #timestamp
    try:
        timestamp = int(datetime.datetime.strptime(formatted_date, '%Y-%m-%d').timestamp())
    except:
        pass

    #who_covidence
    cr_metadata['who_covidence'] = None

    #has_full_text
    cr_metadata['has_full_text'] = None

    #dataset_version
    cr_metadata['dataset_version'] = datetime.datetime.now().timestamp()

    return cr_metadata

def pubmed_parse(response, pmid, journal): # Parse raw metadata from PubMed EFetch result for Vespa

    xml = ET.fromstring(response.content)
    article = xml.find('PubmedArticle').find('MedlineCitation').find('Article')

    pubmed_metadata = dict()

    #title
    pubmed_metadata['title'] = article.find('ArticleTitle').text

    #source
    pubmed_metadata['source'] = 'PubMed'

    #license
    pubmed_metadata['license'] = None

    #datestring
    ArticleDate = article.find('ArticleDate')
    if ArticleDate:
        if ArticleDate.find('Day') != None:
            day = ArticleDate.find('Day').text
            month = ArticleDate.find('Month').text
            if len(month) == 3:
                month = list(calendar.month_abbr).index(month)
            year = ArticleDate.find('Year').text
            formatted_date = "{0}-{1}-{2}".format(year, month, day)
            pubmed_metadata['datestring'] = datetime.datetime.strptime(formatted_date, "%Y-%m-%d")
        elif ArticleDate.find('Month') != None:
            month = ArticleDate.find('Month').text
            if len(month) == 3:
                month = list(calendar.month_abbr).index(month)
            year = ArticleDate.find('Year').text
            formatted_date = "{0}, {1}".format(year, month)
            pubmed_metadata['datestring'] = datetime.datetime.strptime(formatted_date, "%Y-%m")
        elif ArticleDate.find('Year') != None:
            year = ArticleDate.find('Year').text
            formatted_date = "{0}".format(year)
            pubmed_metadata['datestring'] = datetime.datetime.strptime(formatted_date, "%Y")

    #doi, pmcid, and pubmed_id
    IDs = xml.find('PubmedArticle').find('PubmedData').find('ArticleIdList')
    if IDs:
        for id in IDs.iter('ArticleId'):
            if id.attrib['IdType'] == 'doi':
                pubmed_metadata['doi'] = id.text
            if id.attrib['IdType'] == 'pmcid':
                pubmed_metadata['pmcid'] = id.text
    pubmed_metadata['pubmed_id'] = pmid

    #url
    if 'doi' in pubmed_metadata.keys():
        pubmed_metadata['link'] = 'https://doi.org/' + pubmed_metadata['doi']

    #authors
    authors = article.find('AuthorList')
    if authors:
        if authors.attrib['CompleteYN'] == 'Y':
            pubmed_metadata['authors'] = []
            for author in authors.iter('Author'):
                if author.attrib['ValidYN'] == 'Y':
                    author_info = dict()
                    if author.find('ForeName') != None and author.find('LastName') != None:
                        first = author.find('ForeName').text
                        last = author.find('LastName').text
                        name = first + last
                        author_info['Name'] = u'{0} {1}'.format(first, last)
                        pubmed_metadata['authors'].append(author_info)

    #bib_entries
    pubmed_metadata['bib_entries'] = None

    #abstract
    if article.find('Abstract') != None:
        abstract_sections = article.find('Abstract')
        abstract = ""
        for section in abstract_sections.iter('AbstractText'):
            if 'Label' in section.attrib:
                heading = section.attrib['Label'] + ': '
                abstract = abstract + heading + section.text + ' '
            elif section.text != None:
                abstract = abstract + section.text
        pubmed_metadata['abstract'] = abstract

    #journal
    if journal != None:
        pubmed_metadata['journal'] = journal
    else:
        pubmed_metadata['journal'] = article.find('Journal').find('Title').text

    #body_text
    pubmed_metadata['body_text'] = None

    #conclusion
    pubmed_metadata['conclusion'] = None

    #introduction
    pubmed_metadata['introduction'] = None

    #results
    pubmed_metadata['results'] = None

    #discussion
    pubmed_metadata['discussion'] = None

    #methods
    pubmed_metadata['methods'] = None

    #background
    pubmed_metadata['background'] = None

    #Pubmed id
    pubmed_metadata['pubmed_id'] = pmid

    #timestamp
    try:
        timestamp = int(datetime.datetime.strptime(formatted_date, '%Y-%m-%d').timestamp())
    except:
        pass

    #who_covidence
    pubmed_metadata['who_covidence'] = None

    #has_full_text
    pubmed_metadata['has_full_text'] = None

    #dataset_version
    pubmed_metadata['dataset_version'] = datetime.datetime.now().timestamp()

    return pubmed_metadata


def vespa_litcovid_scrape_and_parse():
    titles_pmids_journals = get_LitCovid_Data() # get snapshot of LitCovid data
    for article in titles_pmids_journals:
        # need to add extra condition to check if article already exists in database
        # once LitCovid articles are entered
        
        # scraping and parsing below assumes there are 4 target MongoDB collections:
        #     LitCovid_pubmed_xml
        #     LitCovid_crossref
        #     Vespa_LitCovid_pubmed_parsed
        #     Vespa_LitCovid_crossref_parsed
        pmid = article['pmid']
        pubmed_metadata_xml = pubmed_get(pmid) # get xml metadata from pubmed EFetch
        vespa_pubmed_parsed_data = pubmed_parse(pubmed_metadata_xml, pmid, article['journal'])
        # probably best to add both raw and parsed data to MongoDB collection here... something like:
        # db.LitCovid_pubmed_xml.insert_one(pubmed_metadata_xml)
        # db.Vespa_LitCovid_pubmed_parsed.insert_one(vespa_pubmed_parsed_data)
        if pmid2doi(pmid) != None:
            doi = pmid2doi(pmid)
            crossref_metadata_json = crossref_get(doi) # get json metadata from crossref
            vespa_crossref_parsed_data = crossref_parse(crossref_metadata_json, pmid, article['journal'])
            # probably best to add both raw and parsed data to MongoDB collection here...something like:
            # db.LitCovid_crossref.insert_one(crossref_metadata_json)
            # db.Vespa_LitCovid_crossref_parsed.insert_one(vespa_crossref_parsed_data)
        elif title2doi(article['title']) != None:
            doi = title2doi(article['title'])
            crossref_metadata_json = crossref_get(doi) # get json metadata from crossref
            vespa_crossref_parsed_data = crossref_parse(crossref_metadata_json, pmid, article['journal'])
            # probably best to add both raw and parsed data to MongoDB collection here...something like:
            # db.LitCovid_crossref.insert_one(crossref_metadata_json)
            # db.Vespa_LitCovid_crossref_parsed.insert_one(vespa_crossref_parsed_data)
    return
