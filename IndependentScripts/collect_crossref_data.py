import json
from pprint import pprint
import regex
from datetime import datetime

from common_utils import get_mongo_db
from common_utils import parse_date
from common_utils import parse_names
from common_utils import query_crossref
from common_utils import text_similarity_by_char
from common_utils import valid_a_doi
from common_utils import query_crossref_by_doi

PAPER_COLLECTIONS = {
    'Vespa_CORD_biorxiv_medrxiv_parsed',
    'Vespa_CORD_comm_use_subset_parsed',
    'Vespa_CORD_custom_license_parsed',
    'Vespa_CORD_noncomm_use_subset_parsed',
    'Vespa_Elsevier_corona_parsed',
    'Vespa_LitCovid_parsed',
    'Vespa_biorxiv_medrxiv_parsed',
    'Vespa_chemrxiv_parsed',
    'Vespa_google_form_submissions_parsed',
}

def doi_url_rm_prefix(doi_url):
    doi = doi_url
    tmp_m = regex.match(r'.*doi.org/(.*)', doi_url)
    if tmp_m:
        doi = tmp_m.group(1).strip()
    return doi

def collect_crossref_data(mongo_db):
    error_doc = []
    for col_name in mongo_db.collection_names():
        if col_name not in PAPER_COLLECTIONS:
            continue
        print('col_name', col_name)
        col = mongo_db[col_name]
        aug_col = mongo_db['aug_data_by_doi']

        # interpret csv_raw_result
        query = col.find(
            {
                'doi': {'$exists': True},
            },
            {
                'doi': True,
            }
        )
        for i, doc in enumerate(query):
            if i%1000 == 0:
                print('collect_crossref_data in {}: {}'.format(col_name, i, ))
            if not (isinstance(doc['doi'], str) and len(doc['doi']) > 0):
                continue
            doi = doi_url_rm_prefix(doc['doi'])
            query_aug = aug_col.find_one({'doi': doi})
            if query_aug:
                continue
            try:
                query_result = query_crossref_by_doi(doi)
            except Exception as e:
                query_result = None
                print(e)
            if query_result is not None:
                aug_col.insert_one({
                    'doi': doi,
                    'crossref_raw_result': query_result,
                    'last_updated': datetime.now(),
                })


if __name__ == '__main__':
    db = get_mongo_db('../config.json')
    print(db.collection_names())

    collect_crossref_data(db)


