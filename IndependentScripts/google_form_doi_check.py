from common_utils import get_mongo_db
from common_utils import valid_a_doi


def valid_existing_doi(mongo_db, col_name):
    error_doi = []
    print('col_name', col_name)
    col = mongo_db[col_name]
    query = col.find({
        'doi': {'$exists': True}
    })
    for doc in query:
        valid = valid_a_doi(doi=doc['doi'], abstract=doc.get('abstract'))
        print(doc['doi'], valid)
        if valid == False:
            error_doi.append(doc['doi'])
        # break
    return error_doi

def foo():
    fake_dois = [
        '10.7326/m20-0504',
        '10.7326/m20-050423423423',
        '10.7326/m20',
        '10.3390/v12010064',
    ]
    for doi in fake_dois:
        valid_a_doi(doi)

if __name__ == '__main__':
    db = get_mongo_db('../config.json')
    print(db.collection_names())

    # foo()

    valid_existing_doi(db, 'google_form_submissions')