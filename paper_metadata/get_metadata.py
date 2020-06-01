import os
import sys

parent_folder = os.path.abspath(
    os.path.join(
        os.path.dirname(__file__),
        '..'
    )
)
print('parent_folder', parent_folder)
if parent_folder not in sys.path:
    sys.path.append(parent_folder)
parser_folder = os.path.join(parent_folder, 'parsers')
if parser_folder not in sys.path:
    sys.path.append(parser_folder)

import json
from pprint import pprint
from paper_metadata.api_crossref import query_crossref_by_doi
from paper_metadata.api_scopus import query_scopus_by_doi
from paper_metadata.api_scopus import change_default_scopus_config
from paper_metadata.parser_crossref import CrossrefParser
from paper_metadata.parser_scopus import ScopusParser
from paper_metadata.metadata_doc import MetadataDocument

crossref_parser = CrossrefParser()
scopus_parser = ScopusParser()

####################################################
# use api
####################################################

def get_api_crossref_metadata_by_doi(doi):
    result = None
    # crossref api
    try:
        query_result = query_crossref_by_doi(doi)
    except Exception as e:
        query_result = None
        print(e)
    if query_result is not None:
        result = crossref_parser.get_parsed_doc(query_result)
    return result

def get_api_scopus_metadata_by_doi(doi):
    result = None
    # scopus api
    try:
        query_result = query_scopus_by_doi(doi)
    except Exception as e:
        query_result = None
        print(e)
    if query_result is not None:
        result = scopus_parser.get_parsed_doc(query_result)
    return result

def get_api_metadata_by_doi(doi):
    """
    get metadata of a paper by it doi.
    Query all the APIs such as crossref, scopes, etc.
    If not found, return None.

    :param doi: (str) doi of paper
    :return: (dict or None) metadata of the paper such as title, authors, etc.
    """
    result = None

    docs = []
    funcs = {
        'crossref': get_api_crossref_metadata_by_doi,
        'scopus': get_api_scopus_metadata_by_doi,
    }
    for k, f in funcs.items():
        r = f(doi)
        if r:
            docs.append(r)
    if len(docs) > 0:
        result = MetadataDocument.merge_docs(docs)

    return result

####################################################
# use db
####################################################

def get_db_crossref_metadata_by_doi(mongo_db, doi):
    result = None

    col_name = 'metadata_from_api'
    col = mongo_db[col_name]

    # crossref db records
    doc = col.find_one(
        {'doi': doi, 'crossref_raw_result': {'$exists': True}}
    )
    if doc:
        result = crossref_parser.get_parsed_doc(doc['crossref_raw_result'])
    return result


def get_db_scopus_metadata_by_doi(mongo_db, doi):
    result = None

    col_name = 'metadata_from_api'
    col = mongo_db[col_name]

    # scopus db records
    doc = col.find_one(
        {'doi': doi, 'scopus_raw_result': {'$exists': True}}
    )
    if doc:
        result = scopus_parser.get_parsed_doc(doc['scopus_raw_result'])
    return result

def get_db_metadata_by_doi(mongo_db, doi):
    """
    get metadata of a paper by it doi.
    Query existing metadata in COVID database.
    If not found, return None.

    :param mongo_db: (object) a mongo_db object to fetch data from COVID database
    :param doi: (str) doi of paper
    :return: (dict or None) metadata of the paper such as title, authors, etc.
    """
    result = None

    docs = []
    funcs = {
        'crossref': get_db_crossref_metadata_by_doi,
        'scopus': get_db_scopus_metadata_by_doi,
    }
    for k, f in funcs.items():
        r = f(mongo_db, doi)
        if r:
            docs.append(r)
    if len(docs) > 0:
        result = MetadataDocument.merge_docs(docs)

    return result


####################################################
# entrance to all
####################################################

# TODO: have a function get_metadata_by_pmid()
def get_metadata_by_doi(mongo_db, doi):
    """
    get metadata of a paper by it doi.
    First query existing metadata in COVID database.
    If not found, query API directly.
    If still not found, return None.

    :param mongo_db: (object) a mongo_db object to fetch data from COVID database
    :param doi: (str) doi of paper
    :return: (dict or None) metadata of the paper such as title, authors, etc.
    """

    # TODO: need a schema and auto type here
    result = None

    result = get_db_metadata_by_doi(mongo_db, doi)

    if result is None:
        result = get_api_metadata_by_doi(doi)

    return result


if __name__ == '__main__':
    from paper_metadata.common_utils import get_mongo_db

    db = get_mongo_db('../config.json')
    print(db.collection_names())

    # scrape scopus
    # scopus need some complex api setup
    with open('../config.json', 'r') as fr:
       credentials = json.load(fr)

    change_default_scopus_config(
        api_key=credentials['scopus']['api_key']
    )

    # doc = get_db_metadata_by_doi(db, doi='10.1016/j.cell.2020.02.052')
    doc = get_db_metadata_by_doi(db, doi='10.1016/B978-0-12-385034-8.00005-3')
    # doc = get_db_metadata_by_doi(db, doi='10.1016/B978-0-12-385034-8.00005-3')
    # doc = get_api_metadata_by_doi(doi='10.3390/v4061011')
    # doc = get_api_metadata_by_doi(doi='10.1016/B978-0-12-385034-8.00005-3')
    if doc is not None:
        pprint(doc.to_mongo())
        print('doc.publication_date', doc.publication_date)
        print('doc.validate()', doc.validate())
