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
from paper_metadata.parser_crossref import CrossrefParser

crossref_parser = CrossrefParser()

def get_api_metadata_by_doi(doi):
    """
    get metadata of a paper by it doi.
    Query all the APIs such as crossref, scopes, etc.
    If not found, return None.

    :param doi: (str) doi of paper
    :return: (dict or None) metadata of the paper such as title, authors, etc.
    """
    result = None
    try:
        query_result = query_crossref_by_doi(doi)
    except Exception as e:
        query_result = None
        print(e)
    if query_result is not None:
        result = crossref_parser.get_parsed_doc(query_result)
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

    col_name = 'metadata_from_api'
    col = mongo_db[col_name]
    doc = col.find_one({'doi': doi})
    if doc:
        result = crossref_parser.get_parsed_doc(doc['crossref_raw_result'])
    return result

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

    # doc = get_db_metadata_by_doi(db, doi='10.1016/j.cell.2020.02.052')
    doc = get_db_metadata_by_doi(db, doi='10.3390/v4061011')
    pprint(json.loads(doc.to_json()))
    print('doc.validate()', doc.validate())
