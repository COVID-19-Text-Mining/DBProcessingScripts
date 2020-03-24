import threading

import requests
import json
import urllib
import glob
from pprint import pprint
import re
from datetime import datetime
import difflib
import Levenshtein
import collections
import numpy as np
from statsmodels.stats.weightstats import DescrStatsW
import seaborn as sns
import multiprocessing as mp
import pandas as pd

from common_utils import get_mongo_db, query_crossref_by_doi, query_doiorg_by_doi, text_similarity_by_char
from common_utils import LEAST_ABS_LEN, FIVE_PERCENT_ABS_LEN, LEAST_ABS_SIMILARITY, IGNORE_BEGIN_END_ABS_SIMILARITY

def valid_a_doi(doi, doc_data=None):
    valid = True

    # check via crossref
    query_result = query_crossref_by_doi(doi)
    if query_result is None:
        valid = False
        print('Unable to find by crossref. doi: {} might be invalid!'.format(doi))
    elif ('abstract' in query_result
        and len(query_result['abstract']) > 0
        and 'abstract' in doc_data
        and len(doc_data['abstract']) > 0
    ):
        similarity = text_similarity_by_char(
            query_result['abstract'],
            doc_data['abstract'],
            quick_mode=False,
            enable_ignore_begin_end=True,
            ignore_begin_end_text_len=FIVE_PERCENT_ABS_LEN,
            ignore_begin_end_similarity=IGNORE_BEGIN_END_ABS_SIMILARITY,
        )
        if not (len(query_result['abstract']) > LEAST_ABS_LEN
            and len(doc_data['abstract']) > LEAST_ABS_LEN
            and similarity > LEAST_ABS_SIMILARITY
        ):
            valid = False
            print('Abstract does not match. doi: {} might be invalid!'.format(doi))

    # pprint(query_result)

    # check via doi.org
    query_result = query_doiorg_by_doi(doi)
    if query_result is None or query_result.reason != 'OK':
        valid = False
        print('Unable to find by doi.org. doi: {} might be invalid!'.format(doi))
    return valid


def valid_existing_doi(mongo_db, col_name):
    print('col_name', col_name)
    col = mongo_db[col_name]
    query = col.find({
        'doi': {'$exists': True}
    })
    doc_1 = query.next()
    # pprint(doc_1)
    for doc in query:
        doc['abstract'] = doc_1['abstract']
        valid = valid_a_doi(doi=doc['doi'], doc_data=doc)
        print(doc['doi'], valid)
        # break

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