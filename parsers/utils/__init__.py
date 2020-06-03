import json
import re
import xml.etree.ElementTree as ET

import requests


def clean_title(title):
    if not title:
        return title
    title = title.split("Running Title")[0]
    title = title.replace("^Running Title: ", "")
    title = title.replace("^Short Title: ", "")
    title = title.replace("^Title: ", "")
    title = title.strip()
    return title


def fix_tailing_whitespace(text):
    cnt = sum(map(len, re.findall(r'(?:[\w.!?,]\s)+', text)))
    if cnt / len(text) > 0.75:
        text = re.sub(r'([^\s])\s', lambda x: x.group(1), text)
        text = re.sub(r'\s+', ' ', text)
    return text


def clean_abstract(abstract):
    if not abstract:
        return abstract

    if 'a b s t r a c t' in abstract:
        abstract = abstract.split('a b s t r a c t')[1]
    abstract = fix_tailing_whitespace(abstract)
    return abstract


def find_references(doi):
    """ Returns the references of a document as a <class 'list'> of <class 'dict'>.
    This is a list of documents cited by the current document.
    """
    if doi is None:
        return None

    references = []
    if doi:
        headers = {
            'User-Agent': 'COVIDScholar Parsers',
            'From': 'jdagdelen@lbl.gov'  # This is another valid field
        }
        response = requests.get(f"https://opencitations.net/index/api/v1/references/{doi}", headers=headers)
        if response:
            try:
                response = response.json()
                references = [{"doi": r['cited'].replace("coci =>", ""), "text": r['cited'].replace("coci =>", "")} for
                              r in response]
            except json.decoder.JSONDecodeError:
                pass

    if references:
        return references
    else:
        return None


def find_cited_by(doi):
    """ Returns the citations of a document as a <class 'list'> of <class 'str'>.
    A list of DOIs of documents that cite this document.
    """
    if doi is None:
        return None

    citations = []
    if doi:
        headers = {
            'User-Agent': 'COVIDScholar Parsers',
            'From': 'jdagdelen@lbl.gov'  # This is another valid field
        }
        response = requests.get(f"https://opencitations.net/index/api/v1/citations/{doi}", headers=headers)
        if response:
            try:
                response = response.json()
                citations = [{"doi": r['citing'].replace("coci =>", ""), "text": r['citing'].replace("coci =>", "")} for
                             r in response]
            except json.decoder.JSONDecodeError:
                pass
    if citations:
        return citations
    else:
        return None


def find_remaining_ids(id):
    """ Returns dictionary containing remaining relevant ids corresponding to
    the input id. Just input doi, pmid, or pmcid; function will return all three.
    Example output:
        {
            doi : 'doi_string',
            pmcid 'pmcid_string',
            pubmed_id : 'pubmed_id_string'
        }
    Returns None for either id if not available. Returns None for both ids if
    input id is None or request fails.
    """

    None_dict = {
        'doi': None,
        'pmcid': None,
        'pubmed_id': None
    }
    if id is None:
        return None_dict

    session = requests.Session()
    try:
        ids_url = 'https://www.ncbi.nlm.nih.gov/pmc/utils/idconv/v1.0/?ids=%s' % id
        response = session.get(ids_url)
        root = ET.fromstring(response.content)
        ids = dict()
    except:
        return None_dict
    record = root.find('record')
    if record is None:
        return None_dict
    if 'doi' in record.attrib:
        ids['doi'] = record.attrib['doi']
    else:
        ids['doi'] = None
    if 'pmcid' in record.attrib:
        ids['pmcid'] = record.attrib['pmcid']
    else:
        ids['pmcid'] = None
    if 'pmid' in record.attrib:
        ids['pubmed_id'] = record.attrib['pmid']
    else:
        ids['pubmed_id'] = None
    return ids
