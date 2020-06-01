import json
from pprint import pprint
import regex
from datetime import datetime

from paper_metadata.common_utils import get_mongo_db
from paper_metadata.api_crossref import query_crossref_by_doi
from paper_metadata.api_scopus import query_scopus_by_doi, change_default_scopus_config

PAPER_COLLECTIONS = {
    # 'Vespa_CORD_biorxiv_medrxiv_parsed',
    # 'Vespa_CORD_comm_use_subset_parsed',
    # 'Vespa_CORD_custom_license_parsed',
    # 'Vespa_CORD_noncomm_use_subset_parsed',
    # 'Vespa_Elsevier_corona_parsed',
    # 'Vespa_LitCovid_parsed',
    # 'Vespa_biorxiv_medrxiv_parsed',
    # 'Vespa_chemrxiv_parsed',
    # 'Vespa_google_form_submissions_parsed',
    #
    # 'CORD_parsed_vespa',
    # 'Dimensions_parsed_vespa',
    # 'Elsevier_parsed_vespa',
    # 'Litcovid_crossref_parsed_vespa',

    'CORD_biorxiv_medrxiv',
    'CORD_comm_use_subset',
    'CORD_custom_license',
    'CORD_noncomm_use_subset',
    'Scraper_connect_biorxiv_org',
    'Dimensions_publications',
    'Dimensions_datasets',
    'google_form_submissions',

    # TODO: Elsevier and LitCovid seem to be special, which is the raw collection?
    'Elsevier_parsed_vespa',
    'Vespa_LitCovid_parsed',
}

def doi_url_rm_prefix(doi_url):
    doi = doi_url
    tmp_m = regex.match(r'.*doi.org/(.*)', doi_url)
    if tmp_m:
        doi = tmp_m.group(1).strip()
    return doi

def collect_crossref_data(mongo_db):
    aug_col = mongo_db['metadata_from_api']
    aug_col.create_index('doi', unique=True)

    for col_name in mongo_db.collection_names():
        if col_name not in PAPER_COLLECTIONS:
            continue
        print('col_name', col_name)
        col = mongo_db[col_name]

        doi_column_name = None
        for doc in col.find({}).limit(100):
            for key in doc:
                if key.lower() == 'doi':
                    doi_column_name = key
                    break
            if doi_column_name is not None:
                break
        if doi_column_name is None:
            continue

        # interpret csv_raw_result
        query = col.find(
            {
                doi_column_name: {'$exists': True},
            },
            {
                doi_column_name: True,
            }
        )
        for i, doc in enumerate(query):
            if i%1000 == 0:
                print('collect_crossref_data in {}: {}'.format(col_name, i, ))

            if not (isinstance(doc[doi_column_name], str) and len(doc[doi_column_name]) > 0):
                continue
            doi = doi_url_rm_prefix(doc[doi_column_name])
            query_aug = aug_col.find_one({
                'doi': doi,
                'crossref_raw_result': {'$exists': True}
            })
            if query_aug:
                continue
            try:
                query_result = query_crossref_by_doi(doi)
            except Exception as e:
                query_result = None
                print(e)
            if query_result is not None:
                aug_col.find_one_and_update(
                    {'doi': doi},
                    {
                        '$set': {
                            'doi': doi,
                            'crossref_raw_result': query_result,
                            'last_updated': datetime.now(),
                        }
                    }
                )

def collect_scopus_data(mongo_db):
    aug_col = mongo_db['metadata_from_api']
    aug_col.create_index('doi', unique=True)

    for col_name in mongo_db.collection_names():
        if col_name != 'Elsevier_parsed_vespa':
            continue
        if col_name not in PAPER_COLLECTIONS:
            continue
        print('col_name', col_name)
        col = mongo_db[col_name]

        doi_column_name = None
        for doc in col.find({}).limit(100):
            for key in doc:
                if key.lower() == 'doi':
                    doi_column_name = key
                    break
            if doi_column_name is not None:
                break
        if doi_column_name is None:
            continue

        # interpret csv_raw_result
        query = col.find(
            {
                doi_column_name: {'$exists': True},
            },
            {
                doi_column_name: True,
            }
        )
        for i, doc in enumerate(query):
            if i%1000 == 0:
                print('collect_crossref_data in {}: {}'.format(col_name, i, ))

            if not (isinstance(doc[doi_column_name], str) and len(doc[doi_column_name]) > 0):
                continue
            doi = doi_url_rm_prefix(doc[doi_column_name])
            query_aug = aug_col.find_one({
                'doi': doi,
                'scopus_raw_result': {'$exists': True}
            })
            if query_aug:
                continue
            try:
                query_result = query_scopus_by_doi(doi)
            except Exception as e:
                query_result = None
                print(e)
            if query_result is not None:
                aug_col.find_one_and_update(
                    {'doi': doi},
                    {
                        '$set': {
                            'doi': doi,
                            'scopus_raw_result': query_result,
                            'last_updated': datetime.now(),
                        }
                    }
                )


if __name__ == '__main__':
    db = get_mongo_db('../config.json')
    print(db.collection_names())

    # scrape crossref
    # collect_crossref_data(db)

    # scrape scopus
    # scopus need some complex api setup
    with open('../config.json', 'r') as fr:
       credentials = json.load(fr)

    change_default_scopus_config(
        api_key=credentials['scopus']['api_key']
    )
    collect_scopus_data(db)


