from pprint import pprint
from api_crossref import query_crossref_by_doi
from parser_crossref import CrossrefParser

crossref_parser = CrossrefParser()

def get_api_metadata_by_doi(doi):
    result = None
    try:
        query_result = query_crossref_by_doi(doi)
    except Exception as e:
        query_result = None
        print(e)
    if query_result is not None:
        result = crossref_parser.parse(query_result)
    return result


def get_db_metadata_by_doi(mongo_db, doi):
    result = None

    col_name = 'crossref_metadata'
    col = mongo_db[col_name]
    doc = col.find_one({'doi': doi})
    if doc:
        result = crossref_parser.parse(doc['crossref_raw_result'])
    return result


def get_metadata_by_doi(mongo_db, doi):

    # TODO: need a schema and auto type here
    result = None

    result = get_db_metadata_by_doi(mongo_db, doi)

    if result is None:
        result = get_api_metadata_by_doi(doi)

    return result


if __name__ == '__main__':
    from common_utils import get_mongo_db

    db = get_mongo_db('../config.json')
    print(db.collection_names())

    doc = get_metadata_by_doi(db, doi='10.1016/j.cell.2020.02.052')
    print(doc)