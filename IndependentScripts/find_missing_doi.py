import requests
import json
import urllib
import glob
from pprint import pprint
import re
from datetime import datetime

from common_utils import get_mongo_db


def doi_existence_stat(mongo_db):
    for col_name in mongo_db.collection_names():
        if col_name not in {
            'CORD_biorxiv_medrxiv',
            'CORD_comm_use_subset',
            'CORD_custom_license',
            'CORD_noncomm_use_subset',
        }:
            continue
        col = mongo_db[col_name]
        query_w_doi = col.find({'doi': {'$exists': True}})
        query_w_doi_len_gt_0 = col.find({
            'doi': {'$exists': True},
            '$where': 'this.doi.length > 0',
        })
        query_w_crossref_tried = col.find({'tried_crossref_doi': True})
        query_wo_doi_title_empty = col.find({
            'doi': {'$exists': False},
            'metadata': {'$exists': True},
            '$where': 'this.metadata.title.length == 0',
        })
        print('col_name', col_name)
        print('col.count()', col.count())
        print('query_w_doi', query_w_doi.count())
        print('query_w_doi_len_gt_0', query_w_doi_len_gt_0.count())
        print('query_w_crossref_tried', query_w_crossref_tried.count())
        print('query_wo_doi_title_empty', query_wo_doi_title_empty.count())
        print()

def clean_title(title):
    clean_title = title.split("Running Title")[0]
    clean_title = re.sub('( [0-9])*$', '', clean_title)
    clean_title = clean_title.replace("Running Title: ", "")
    clean_title = clean_title.replace("Short Title: ", "")
    clean_title = clean_title.replace("Title: ", "")
    clean_title = clean_title.strip()
    return clean_title


def foo(mongo_db):
    for col_name in mongo_db.collection_names():
        if col_name != 'CORD_noncomm_use_subset':
            continue
        if col_name not in {
            'CORD_biorxiv_medrxiv',
            'CORD_comm_use_subset',
            'CORD_custom_license',
            'CORD_noncomm_use_subset',
        }:
            continue
        col = mongo_db[col_name]
        query = col.find(
            {
                "tried_crossref_doi" : { "$exists" : False },
                "doi" : { "$exists" : False }
            }
        )
        query = list(query)
        print('len(query)', len(query))
        for i, doc in enumerate(query):
            if i%100==0:
                print('processing the {}th out of {}'.format(i, len(query)))
            doc_updated = False

            # get metadata
            metadata = None
            if ('metadata' in doc):
                metadata = doc['metadata']
            else:
                # let's supporse metadata is always used first
                # TODO: we can also use abstract when metadata is not available
                continue


            # get title
            title = None
            if metadata is not None:
                if not ('title' in metadata
                    and isinstance(metadata['title'], str)
                    and len(metadata['title'].strip()) > 0
                ):
                    # doc w/o is minor part let's ignore them first
                    # TODO: we can also use abstract when metadata is not available
                    continue
                print('raw_title: ', metadata['title'])
                title = clean_title(metadata['title'])


            # get author
            author_names = None
            if metadata is not None:
                try:
                    author_names = ",".join([a['last'] for a in metadata['authors']])
                except KeyError:
                    author_names = None

            # query cross_ref
            query_url = 'https://api.crossref.org/works'
            query_params = {}
            if title:
                # after some experiments, we use pass the query value in plain str rather than html str
                # therefore, use title instead of urllib.parse.quote_plus(title)
                query_params['query.bibliographic'] = title
            # TODO: might need to double check if exact title matching be perfect (author might be different?)
            # TODO: might be wrong here need to clean db when only author info is used to retrieve data
            elif author_names:
                query_params['query.bibliographic'] = author_names
            # TODO: might also use email to search?

            cross_ref_results = requests.get(
                query_url,
                params=query_params,
            )
            try:
                cross_ref_results = cross_ref_results.json()
            except Exception as e:
                print('query result cannot be jsonified!')
                print('cross_ref_results.text', cross_ref_results.text)
                print('cross_ref_results.status_code', cross_ref_results.status_code)
                print('cross_ref_results.reason', cross_ref_results.reason)
                print()

            # filter out empty query results
            if not ('message' in cross_ref_results
                and 'items' in cross_ref_results['message']
                and isinstance(cross_ref_results['message']['items'], list)
                and len(cross_ref_results['message']['items']) > 0
            ):
                print('EMPTY RESULT')
                pprint(cross_ref_results)
                print()
                continue
            else:
                cross_ref_results = cross_ref_results['message']['items']

            # filter out query results without title
            # TODO: maybe abtract is available
            #  use item['abstract']
            cross_ref_results = list(filter(
                lambda x: (
                        'title' in x
                        and isinstance(x['title'], list)
                        and len(x['title']) > 0
                ),
                cross_ref_results
            ))
            # exit()

            # print('title', title)
            # print("metadata['title']", metadata['title'])
            # print('author_names', author_names)
            # pprint(r.json())
            # exit()

            # match by title directly
            matched_item = None
            if matched_item is None:
                for item in cross_ref_results:
                    if len(item['title']) > 1:
                        print("len(item['title'])", len(item['title']))
                    if item['title'][0] == title:
                        matched_item = item
                        break

            # match by revised title
            if matched_item is None:
                title = re.sub(' [0-9] ', ' ', title)
                for item in cross_ref_results:
                    if item['title'][0] == title:
                        matched_item = item
                        break


            # update doi found
            if matched_item is not None:
                print("FOUND")
                col.find_one_and_update(
                    {"_id": doc['_id']},
                    {
                        "$set": {
                            "doi": item['DOI'],
                            'tried_crossref_doi': True,
                            'crossref_raw_result': item,
                            'last_updated': datetime.now(),
                        }
                    }
                )
                doc_updated = True
            # else:
            #     print('query_params', query_params)
            #     print('\n'.join([x['title'][0] for x in cross_ref_results]))

            # mark tried even if doi is not found but searching is completed
            if not doc_updated:
                col.find_one_and_update(
                    {"_id": doc['_id']},
                    {
                        "$set": {
                            "tried_crossref_doi": True,
                            'last_updated': datetime.now(),
                        }
                    }
                )
            print()


if __name__ == '__main__':
    db = get_mongo_db('../config.json')
    print(db.collection_names())
    doi_existence_stat(db)
    # foo(db)