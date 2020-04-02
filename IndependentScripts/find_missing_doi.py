import threading

from pprint import pprint
import re
from datetime import datetime
import collections
import numpy as np
from statsmodels.stats.weightstats import DescrStatsW
import seaborn as sns
import multiprocessing as mp
import pandas as pd

from common_utils import get_mongo_db, parse_date, parse_names, query_crossref, text_similarity_by_char
from common_utils import LEAST_TITLE_LEN, FIVE_PERCENT_TITLE_LEN, LEAST_TITLE_SIMILARITY, IGNORE_BEGIN_END_TITLE_SIMILARITY
from common_utils import LEAST_ABS_LEN, FIVE_PERCENT_ABS_LEN, LEAST_ABS_SIMILARITY, IGNORE_BEGIN_END_ABS_SIMILARITY

PAPER_COLLECTIONS = {
    'CORD_biorxiv_medrxiv',
    'CORD_comm_use_subset',
    'CORD_custom_license',
    'CORD_noncomm_use_subset',
}


#######################################
# functions for stats
#######################################

def doi_existence_stat(mongo_db):
    for col_name in mongo_db.collection_names():
        if col_name not in PAPER_COLLECTIONS:
            continue
        col = mongo_db[col_name]
        query_w_doi = col.find({'doi': {'$exists': True}})
        query_w_doi_len_gt_0 = col.find({
            'doi': {'$exists': True},
            '$where': 'this.doi.length > 0',
        })
        query_w_crossref_tried = col.find({'tried_crossref_doi': True})
        query_w_csv_tried = col.find({'tried_csv_doi': True})
        query_w_cord_id = col.find({'paper_id': {'$exists': True}})

        query_wo_doi_no_cr_csv = col.find({
            'doi': {'$exists': False},
            'crossref_raw_result': {'$exists': False},
            'csv_raw_result': {'$exists': False},
        })
        query_wo_doi_title_empty = col.find({
            'doi': {'$exists': False},
            'metadata': {'$exists': True},
            '$where': 'this.metadata.title.length == 0',
        })
        query_wo_doi_abs_empty = col.find({
            'doi': {'$exists': False},
            'abstract': {'$exists': True},
            '$where': 'this.abstract.length == 0',
        })
        query_wo_doi_title_abs_empty = col.find({
            'doi': {'$exists': False},
            'metadata': {'$exists': True},
            'abstract': {'$exists': True},
            '$where': 'this.metadata.title.length == 0 & this.abstract.length == 0',
        })
        query_wo_doi_cord_id_empty = col.find({
            'doi': {'$exists': False},
            'paper_id': {'$exists': False},
        })

        print('col_name', col_name)
        print('col.count()', col.count())
        print('query_w_doi', query_w_doi.count())
        print('query_w_doi_len_gt_0', query_w_doi_len_gt_0.count())
        print('query_w_crossref_tried', query_w_crossref_tried.count())
        print('query_w_csv_tried', query_w_csv_tried.count())
        print('query_w_cord_id', query_w_cord_id.count())
        print('query_wo_doi_no_cr_csv', query_wo_doi_no_cr_csv.count())
        print('query_wo_doi_title_empty', query_wo_doi_title_empty.count())
        print('query_wo_doi_abs_empty', query_wo_doi_abs_empty.count())
        print('query_wo_doi_title_abs_empty', query_wo_doi_title_abs_empty.count())
        print('query_wo_doi_cord_id_empty', query_wo_doi_cord_id_empty.count())
        print()


def title_len_stat(mongo_db):
    len_counter_db = collections.Counter()
    len_counter_cr = collections.Counter()
    for col_name in mongo_db.collection_names():
        if col_name not in PAPER_COLLECTIONS:
            continue
        col = mongo_db[col_name]
        query_w_doi = col.find({'doi': {'$exists': True}})
        for doc in query_w_doi:
            if ('metadata' in doc
                    and 'title' in doc['metadata']
                    and isinstance(doc['metadata']['title'], str)
            ):
                len_counter_db[len(doc['metadata']['title'])] += 1
            if ('crossref_raw_result' in doc
                    and 'title' in doc['crossref_raw_result']
                    and isinstance(doc['crossref_raw_result']['title'], list)
                    and len(doc['crossref_raw_result']['title']) == 1
            ):
                len_counter_cr[len(doc['crossref_raw_result']['title'][0])] += 1

    # stat for db titles
    sorted_len = sorted(len_counter_db.keys())
    weights = [len_counter_db[l] for l in sorted_len]
    weighted_stats = DescrStatsW(sorted_len, weights=weights)
    sns.barplot(sorted_len, weights)
    percentile = weighted_stats.quantile(
        probs=[0, 0.01, 0.02, 0.03, 0.04, 0.05, 0.1, 0.25, 0.5, 0.75, 0.95, 0.97, 0.99]
    )
    print('len_counter_db')
    pprint(len_counter_db)
    print('weighted_stats.mean', weighted_stats.mean)
    print('weighted_stats.std', weighted_stats.std)
    print('percentile')
    print(percentile)

    # stat for cr titles
    sorted_len = sorted(len_counter_cr.keys())
    weights = [len_counter_cr[l] for l in sorted_len]
    weighted_stats = DescrStatsW(sorted_len, weights=weights)
    #     sns.barplot(sorted_len, weights)
    percentile = weighted_stats.quantile(
        probs=[0, 0.01, 0.02, 0.03, 0.04, 0.05, 0.1, 0.25, 0.5, 0.75, 0.95, 0.97, 0.99]
    )
    print('len_counter_cr')
    pprint(len_counter_cr)
    print('weighted_stats.mean', weighted_stats.mean)
    print('weighted_stats.std', weighted_stats.std)
    print('percentile')
    print(percentile)

    return len_counter_db, len_counter_cr


def abs_len_stat(mongo_db):
    len_counter = collections.Counter()
    for col_name in mongo_db.collection_names():
        if col_name not in PAPER_COLLECTIONS:
            continue
        col = mongo_db[col_name]
        query_w_doi = col.find({'abstract': {'$exists': True}})
        for doc in query_w_doi:
            # get abstract
            abstract = None
            if 'abstract' in doc and len(doc['abstract']) > 0:
                abstract = ''
                for fragment in doc['abstract']:
                    if ('text' in fragment
                            and isinstance(fragment['text'], str)
                            and len(fragment['text']) > 0
                    ):
                        abstract += fragment['text'].strip() + ' '

                abstract = abstract.strip()
                if len(abstract) == 0:
                    abstract = None

            if abstract is not None:
                #                 print(abstract)
                len_counter[len(abstract)] += 1

    # stat for db abs
    sorted_len = sorted(len_counter.keys())
    weights = [len_counter[l] for l in sorted_len]
    weighted_stats = DescrStatsW(sorted_len, weights=weights)
    sns.barplot(sorted_len, weights)
    percentile = weighted_stats.quantile(
        probs=[0, 0.01, 0.02, 0.03, 0.04, 0.05, 0.1, 0.25, 0.5, 0.75, 0.95, 0.97, 0.99]
    )
    print('len_counter')
    pprint(len_counter)
    print('weighted_stats.mean', weighted_stats.mean)
    print('weighted_stats.std', weighted_stats.std)
    print('percentile')
    print(percentile)

    return len_counter


def check_doc_wo_doi(mongo_db):
    for col_name in mongo_db.collection_names():
        if col_name not in PAPER_COLLECTIONS:
            continue
        print('col_name', col_name)
        col = mongo_db[col_name]
        query = col.find({
            'doi': {'$exists': False},
            'crossref_raw_result': {'$exists': False},
            'csv_raw_result': {'$exists': False},
        })

        print('len(query)', query.count())
        info_counter = collections.Counter()
        for doc in query:
            # get metadata
            metadata = None
            if ('metadata' in doc):
                metadata = doc['metadata']

            # get title
            title = None
            raw_title = None
            if metadata is not None:
                if ('title' in metadata
                        and isinstance(metadata['title'], str)
                        and len(metadata['title'].strip()) > 0
                ):
                    raw_title = metadata['title']
                    title = clean_title(raw_title)

            # get abstract
            abstract = None
            if 'abstract' in doc and len(doc['abstract']) > 0:
                abstract = ''
                for fragment in doc['abstract']:
                    if ('text' in fragment
                        and isinstance(fragment['text'], str)
                        and len(fragment['text']) > 0
                    ):
                        abstract += fragment['text'].strip() + ' '

                abstract = abstract.strip()
                if len(abstract) == 0:
                    abstract = None

            info_counter['all'] += 1
            if raw_title is not None or abstract is not None:
                info_counter['w_title_or_abs'] += 1
                if raw_title is not None:
                    info_counter['w_title'] += 1
                    print('raw_title', raw_title)
                if abstract is not None:
                    info_counter['w_abs'] += 1
                    print('abstract', abstract)
                print()
        print('info_counter')
        print(info_counter)


#######################################
# functions for post processing
#######################################

def add_useful_fields(mongo_db):
    for col_name in mongo_db.collection_names():
        # if col_name != 'CORD_custom_license':
        #     continue
        if col_name not in PAPER_COLLECTIONS:
            continue
        print('col_name', col_name)
        col = mongo_db[col_name]

        # interpret csv_raw_result
        query = col.find(
            {
                "csv_raw_result" : { "$exists" : True },
            },
        )
        query = list(query)
        print('len(query) csv_raw_result', len(query))
        for doc in query:
            set_params = {}
            # pmcid
            if ('pmcid' in doc['csv_raw_result']
                and isinstance(doc['csv_raw_result']['pmcid'], str)
                and len(doc['csv_raw_result']['pmcid'].strip()) > 0
            ):
                set_params['pmcid'] = doc['csv_raw_result']['pmcid'].strip()

            # pubmed_id
            if ('pubmed_id' in doc['csv_raw_result']
                and isinstance(doc['csv_raw_result']['pubmed_id'], str)
                and len(doc['csv_raw_result']['pubmed_id'].strip()) > 0
            ):
                set_params['pubmed_id'] = doc['csv_raw_result']['pubmed_id'].strip()

            # pubmed_id
            if ('Microsoft Academic Paper ID' in doc['csv_raw_result']
                and isinstance(doc['csv_raw_result']['Microsoft Academic Paper ID'], str)
                and len(doc['csv_raw_result']['Microsoft Academic Paper ID'].strip()) > 0
            ):
                set_params['microsoft_academic_paper_id'] = doc['csv_raw_result']['Microsoft Academic Paper ID'].strip()

            # journal_name
            if ('journal' in doc['csv_raw_result']
                and isinstance(doc['csv_raw_result']['journal'], str)
                and len(doc['csv_raw_result']['journal'].strip()) > 0
            ):
                set_params['journal_name'] = doc['csv_raw_result']['journal'].strip()

            # publish_date
            if ('publish_time' in doc['csv_raw_result']
                and isinstance(doc['csv_raw_result']['publish_time'], str)
                and len(doc['csv_raw_result']['publish_time'].strip()) > 0
            ):
                set_params['publish_date'] = parse_date(doc['csv_raw_result']['publish_time'].strip())

            # update doc
            if len(set_params) > 0:
                try:
                    col.find_one_and_update(
                        {"_id": doc['_id']},
                        {
                            "$set": set_params,
                        }
                    )
                except Exception as e:
                    print('doc _id', doc['_id'])
                    print('set_params', set_params)
                    print(e)
                    raise e

        # to validate crossref_raw_result and cr_raw_result with each other
        # largely the same
        # some of the doi's are different so I manually checked them
        # then I realized that's doi's from publisher website and preprint
        # websites so both are correct

        # interpret crossref_raw_result
        query = col.find(
            {
                "crossref_raw_result" : { "$exists" : True },
            },
        )
        query = list(query)
        print('len(query) crossref_raw_result', len(query))
        for doc in query:
            set_params = {}
            # journal_name
            journal_name = None
            if (journal_name is None
                and 'container-title' in doc['crossref_raw_result']
                and isinstance(doc['crossref_raw_result']['container-title'], list)
                and len(doc['crossref_raw_result']['container-title']) == 1
            ):
                journal_name = doc['crossref_raw_result']['container-title'][0]
            if (journal_name is None
                and 'short-container-title' in doc['crossref_raw_result']
                and isinstance(doc['crossref_raw_result']['short-container-title'], list)
                and len(doc['crossref_raw_result']['short-container-title']) == 1
            ):
                journal_name = doc['crossref_raw_result']['short-container-title'][0]
            if journal_name is not None:
                set_params['journal_name'] = journal_name

            # ISSN
            if ('ISSN' in doc['crossref_raw_result']
                and isinstance(doc['crossref_raw_result']['ISSN'], list)
                and len(doc['crossref_raw_result']['ISSN']) == 1
            ):
                set_params['ISSN'] = doc['crossref_raw_result']['ISSN'][0]

            # publish_date
            publish_date = None
            if (publish_date is None
                and 'issued' in doc['crossref_raw_result']
                and 'date-parts' in doc['crossref_raw_result']['issued']
                and isinstance(doc['crossref_raw_result']['issued']['date-parts'], list)
                and len(doc['crossref_raw_result']['issued']['date-parts']) == 1
                and len(doc['crossref_raw_result']['issued']['date-parts'][0]) > 0
            ):
                publish_date = doc['crossref_raw_result']['issued']['date-parts'][0]
            if (publish_date is None
                and 'published-online' in doc['crossref_raw_result']
                and 'date-parts' in doc['crossref_raw_result']['published-online']
                and isinstance(doc['crossref_raw_result']['published-online']['date-parts'], list)
                and len(doc['crossref_raw_result']['published-online']['date-parts']) == 1
                and len(doc['crossref_raw_result']['published-online']['date-parts'][0]) > 0
            ):
                publish_date = doc['crossref_raw_result']['published-online']['date-parts'][0]
            if (publish_date is None
                and 'published-print' in doc['crossref_raw_result']
                and 'date-parts' in doc['crossref_raw_result']['published-print']
                and isinstance(doc['crossref_raw_result']['published-print']['date-parts'], list)
                and len(doc['crossref_raw_result']['published-print']['date-parts']) == 1
                and len(doc['crossref_raw_result']['published-print']['date-parts'][0]) > 0
            ):
                publish_date = doc['crossref_raw_result']['published-print']['date-parts'][0]
            if publish_date is not None:
                set_params['publish_date'] = publish_date

            # reference
            if ('reference' in doc['crossref_raw_result']
                and isinstance(doc['crossref_raw_result']['reference'], list)
                and len(doc['crossref_raw_result']['reference']) > 0
            ):
                crossref_reference = []
                for ref in doc['crossref_raw_result']['reference']:
                    if ('DOI' in ref
                        and isinstance(ref['DOI'], str)
                        and len(ref['DOI']) > 0
                    ):
                        crossref_reference.append(ref['DOI'])
                if len(crossref_reference) > 0:
                    set_params['crossref_reference'] = crossref_reference

            # update doc
            if len(set_params) > 0:
                try:
                    col.find_one_and_update(
                        {"_id": doc['_id']},
                        {
                            "$set": set_params,
                        }
                    )
                except Exception as e:
                    print('doc _id', doc['_id'])
                    print('set_params', set_params)
                    print(e)
                    raise e


def assign_suggested_doi(mongo_db):
    for col_name in mongo_db.collection_names():
        # if col_name != 'CORD_custom_license':
        #     continue
        if col_name not in PAPER_COLLECTIONS:
            continue
        print('col_name', col_name)
        col = mongo_db[col_name]

        query = col.find({})
        for i, doc in enumerate(query):
            if i%1000 == 0:
                print('{}th doc updating'.format(i))
            # # comment out this if hash strategy is changed
            # if 'suggested_id' in doc:
            #     continue
            suggested_id = None
            if 'doi' in doc and len(doc['doi']) > 0:
                suggested_id = {
                    'id': doc['doi'],
                    'source': 'doi',
                }
            elif 'pubmed_id' in doc and len(doc['pubmed_id']) > 0:
                suggested_id = {
                    'id': doc['pubmed_id'],
                    'source': 'pubmed_id',
                }
            elif 'pmcid' in doc and len(doc['pmcid']) > 0 :
                suggested_id = {
                    'id': doc['pmcid'],
                    'source': 'pmcid',
                }
            elif 'microsoft_academic_paper_id' in doc and len(doc['microsoft_academic_paper_id']) > 0:
                suggested_id = {
                    'id': doc['microsoft_academic_paper_id'],
                    'source': 'microsoft_academic_paper_id',
                }
            else:
                suggested_id = {
                    'id': hash(str(doc)),
                    'source': 'hash(str(doc))',
                }
            if suggested_id is not None:
                try:
                    col.find_one_and_update(
                        {"_id": doc['_id']},
                        {
                            "$set": {
                                'suggested_id': suggested_id,
                            },
                        }
                    )
                except Exception as e:
                    print('doc _id', doc['_id'])
                    print('suggested_id', suggested_id)
                    print(e)
                    raise e


#######################################
# functions for doi searching task
#######################################

def clean_title(title):
    clean_title = title
    clean_title = clean_title.lower()
    clean_title = clean_title.strip()
    return clean_title

def correct_pd_dict(input_dict):
    """
        Correct the encoding of python dictionaries so they can be encoded to mongodb
        https://stackoverflow.com/questions/30098263/inserting-a-document-with-
        pymongo-invaliddocument-cannot-encode-object
        inputs
        -------
        input_dict : dictionary instance to add as document
        output
        -------
        output_dict : new dictionary with (hopefully) corrected encodings
    """

    output_dict = {}
    for key1, val1 in input_dict.items():
        # Nested dictionaries
        if isinstance(val1, dict):
            val1 = correct_pd_dict(val1)

        if isinstance(val1, np.bool_):
            val1 = bool(val1)

        if isinstance(val1, np.int64):
            val1 = int(val1)

        if isinstance(val1, np.float64):
            val1 = float(val1)

        if isinstance(val1, set):
            val1 = list(val1)

        output_dict[key1] = val1

    return output_dict


def compare_author_names(name_list_1, name_list_2):
    all_first_names_1 = [x['last'] for x in name_list_1]
    all_first_names_1 = list(filter(lambda x: x is not None, all_first_names_1))
    all_first_names_1 = [x.strip() for x in all_first_names_1]
    all_first_names_1 = set(filter(lambda x: len(x)>=2, all_first_names_1))

    all_first_names_2 = [x['last'] for x in name_list_2]
    all_first_names_2 = list(filter(lambda x: x is not None, all_first_names_2))
    all_first_names_2 = [x.strip() for x in all_first_names_2]
    all_first_names_2 = set(filter(lambda x: len(x)>=2, all_first_names_2))

    similarity = len(all_first_names_1&all_first_names_2)/max(
        len(all_first_names_1), len(all_first_names_2), 1
    )
    return similarity>0.6


def doi_match_a_batch_by_crossref(task_batch):
    mongo_db = get_mongo_db('../config.json')
    for i, task in enumerate(task_batch):
        if i % 100 == 0:
            print('thread', threading.currentThread().getName())
            print('processing the {}th out of {}'.format(i, len(task_batch)))
        col = mongo_db[task['col_name']]

        # get doc
        doc = col.find_one({'_id': task['_id']})
        if doc is None:
            continue

        doc_updated = False

        # get metadata
        metadata = None
        if ('metadata' in doc):
            metadata = doc['metadata']

        # get title
        title = None
        raw_title = None
        if metadata is not None:
            if ('title' in metadata
                and isinstance(metadata['title'], str)
                and len(metadata['title'].strip()) > 0
            ):
                raw_title = metadata['title']
                title = clean_title(raw_title)

        # get author
        author_names = None
        if metadata is not None:
            author_names = metadata.get('authors')
            if not (isinstance(author_names, list) and len(author_names) > 0):
                author_names = None

        # get abstract
        abstract = None
        if 'abstract' in doc and len(doc['abstract']) > 0:
            abstract = ''
            for fragment in doc['abstract']:
                if ('text' in fragment
                    and isinstance(fragment['text'], str)
                    and len(fragment['text']) > 0
                ):
                    abstract += fragment['text'].strip() + ' '

            abstract = abstract.strip()
            if len(abstract) == 0:
                abstract = None

        # query crossref
        crossref_results = []
        if title:
            # after some experiments, we use pass the query value in plain str rather than html str
            # therefore, use title instead of urllib.parse.quote_plus(title)
            query_params = {
                'sort': 'relevance',
                'order': 'desc',
                'query.bibliographic': title,
            }
            try:
                query_results = query_crossref(query_params)
            except Exception as e:
                query_results = None
                print(e)
            if query_results is not None:
                crossref_results.extend(query_results)

        if author_names:
            query_params = {
                'sort': 'relevance',
                'order': 'desc',
                'query.bibliographic': ', '.join([x['last'] for x in author_names]),
            }
            try:
                query_results = query_crossref(query_params)
            except Exception as e:
                query_results = None
                print(e)
            if query_results is not None:
                crossref_results.extend(query_results)

        # TODO: might need to double check if exact title matching be perfect (also, author might be different?)

        # filter out query results without DOI
        crossref_results = list(filter(
            lambda x: (
                'DOI' in x
                and isinstance(x['DOI'], str)
                and len(x['DOI']) > 0
            ),
            crossref_results
        ))

        # filter out query results without title or abstract
        crossref_results = list(filter(
            lambda x: (
                (
                    'title' in x
                    and isinstance(x['title'], list)
                    and len(x['title']) > 0
                ) or (
                    'abstract' in x
                    and isinstance(x['abstract'], str)
                    and len(x['abstract']) > 0
                )
            ),
            crossref_results
        ))

        # match by title directly
        matched_item = None
        matched_candidates = []

        if title is not None and matched_item is None:
            for item in crossref_results:
                if not ('title' in item
                    and isinstance(item['title'], list)
                    and len(item['title']) > 0
                ):
                    continue
                if len(item['title']) != 1:
                    print("len(item['title']) != 1", len(item['title']))
                cr_title = clean_title(item['title'][0])
                similarity = text_similarity_by_char(
                    cr_title,
                    title,
                    enable_ignore_begin_end=True,
                    ignore_begin_end_text_len=FIVE_PERCENT_TITLE_LEN,
                    ignore_begin_end_similarity=IGNORE_BEGIN_END_TITLE_SIMILARITY,
                )
                if (len(cr_title) > LEAST_TITLE_LEN
                    and len(title) > LEAST_TITLE_LEN
                    and similarity > LEAST_TITLE_SIMILARITY):
                    print('raw_title: ', raw_title)
                    print('title', title)
                    print("cr_title", cr_title)
                    print('similarity', similarity)
                    print()
                    matched_item = item
                    break
                elif (len(cr_title) > LEAST_TITLE_LEN
                    and len(title) > LEAST_TITLE_LEN
                    and similarity > 0.5):
                    matched_candidates.insert(0, item)

        # match by abstract
        if abstract is not None and matched_item is None:
            for item in crossref_results:
                if not ('abstract' in item
                    and isinstance(item['abstract'], str)
                    and len(item['abstract']) > 0
                ):
                    continue
                cr_abstract = item['abstract']
                similarity = text_similarity_by_char(
                    cr_abstract,
                    abstract,
                    enable_ignore_begin_end=True,
                    ignore_begin_end_text_len=FIVE_PERCENT_ABS_LEN,
                    ignore_begin_end_similarity=IGNORE_BEGIN_END_ABS_SIMILARITY,
                )
                if (len(cr_abstract) > LEAST_ABS_LEN
                    and len(abstract) > LEAST_ABS_LEN
                    and similarity > LEAST_ABS_SIMILARITY):
                    print('abstract: ', abstract)
                    print("cr_abstract", cr_abstract)
                    print('similarity', similarity)
                    print()
                    matched_item = item
                    break
                elif (len(cr_abstract) > LEAST_ABS_LEN
                    and len(abstract) > LEAST_ABS_LEN
                    and similarity > 0.5):
                    matched_candidates.insert(0, item)

        if (matched_item is None and len(matched_candidates) > 0 and author_names is not None):
            # match by author
            for candidate in matched_candidates:
                if not ('author' in candidate
                    and isinstance(candidate['author'], list)
                    and len(candidate['author']) > 0
                ) :
                    continue
                names_parsed = parse_names(candidate['author'])
                name_cmp_result = compare_author_names(author_names, names_parsed)
                print('raw_title: ', raw_title)
                print("candidate['title']", candidate.get('title'))
                print('abstract', abstract)
                print("candidate['abstract']", candidate.get('abstract'))
                print('author_names', [{'first': x['first'], 'last': x['last']} for x in author_names])
                print("candidate['author']", candidate.get('author'))
                print('name_cmp_result', name_cmp_result)
                print()
                if name_cmp_result:
                    matched_item = candidate
                    break

        if matched_item is None and len(matched_candidates) == 0:
            print('no similar and no candidates!')
            print('raw_title: ', raw_title)
            print('abstract', abstract)
            if author_names:
                print('author_names', [{'first': x['first'], 'last': x['last']} for x in author_names])
            else:
                print('author_names', author_names)
            print()

        # update db
        set_params = {
            "tried_crossref_doi": True,
            'last_updated': datetime.now(),
        }
        if matched_item is not None:
            print("FOUND")
            print()
            set_params['crossref_raw_result'] = matched_item
            if (matched_item.get('DOI')
                and isinstance(matched_item['DOI'], str)
                and len(matched_item['DOI'].strip()) > 0
            ):
                set_params['doi'] = matched_item['DOI'].strip()

            doc_updated = True

        try:
            col.find_one_and_update(
                {"_id": doc['_id']},
                {
                    "$set": set_params,
                }
            )
        except Exception as e:
            print('matched_item')
            pprint(matched_item)
            print(e)
            raise e

def doi_match_a_batch_by_csv_new(task_batch):
    mongo_db = get_mongo_db('../config.json')
    csv_data = pd.read_csv(
        '../rsc/metadata.csv',
        dtype={
            'pubmed_id': str,
            'pmcid': str,
            'publish_time': str,
            'Microsoft Academic Paper ID': str,
        }
    )
    csv_data = csv_data.fillna('')
    csv_data['title'] = csv_data['title'].str.lower()
    data = csv_data[csv_data['sha']!='']
    print('data.shape', data.shape)
    for i, task in enumerate(task_batch):
        if i % 10 == 0:
            print('thread', threading.currentThread().getName())
            print('processing the {}th out of {}'.format(i, len(task_batch)))
        col = mongo_db[task['col_name']]

        # get doc
        doc = col.find_one({'_id': task['_id']})
        if doc is None:
            continue

        doc_updated = False

        # get cord_id
        cord_id = None
        if ('paper_id' in doc and isinstance(doc['paper_id'], str) and len(doc['paper_id']) > 0):
            cord_id = doc['paper_id']

        # query csv_data
        matched_item = None

        # match by title
        if cord_id is not None and matched_item is None:
            data_w_cord_id = csv_data[csv_data['sha'] == cord_id]

            if len(data_w_cord_id) == 1:
                # print('raw_title: ', raw_title)
                # print('title', title)
                # print("csv_title", sorted_data.iloc[0]['title'])
                # print('similarity', sorted_similarity.iloc[0])
                # print(sorted_similarity.head(10))
                # print('len(raw_title)', len(raw_title))
                # print('doi', sorted_data.iloc[0]['doi'])
                # print()
                #
                # if (len(title) > LEAST_TITLE_LEN
                #     and len(sorted_data.iloc[0]['title']) > LEAST_TITLE_LEN
                #     and sorted_similarity.iloc[0] > LEAST_TITLE_SIMILARITY):
                matched_item = correct_pd_dict(data_w_cord_id.iloc[0].to_dict())


            elif len(data_w_cord_id) > 1:
                print('more than 1 entries matched!')
                print('cord_id', cord_id)
                print(', '.join(list(data_w_cord_id['sha'])))

            else:
                print('no entry matched!')
                print('cord_id', cord_id)


        if matched_item is None:
            print('no entry matched!')
            print('cord_id', cord_id)
            print()

        # update db
        set_params = {
            "tried_csv_doi": True,
            'last_updated': datetime.now(),
        }

        # update doi found
        if matched_item is not None:
            print("FOUND")
            print()
            set_params['csv_raw_result'] = matched_item
            if (matched_item.get('doi')
                and isinstance(matched_item['doi'], str)
                and len(matched_item['doi'].strip())>0
            ):
                set_params['doi'] = matched_item['doi'].strip()

            doc_updated = True

        try:
            col.find_one_and_update(
                {"_id": doc['_id']},
                {
                    "$set": set_params,
                }
            )
        except Exception as e:
            print('matched_item')
            pprint(matched_item)
            print(e)
            raise e


def doi_match_a_batch_by_csv(task_batch):
    mongo_db = get_mongo_db('../config.json')
    csv_data = pd.read_csv(
        '../rsc/metadata.csv',
        dtype={
            'pubmed_id': str,
            'pmcid': str,
            'publish_time': str,
            'Microsoft Academic Paper ID': str,
        }
    )
    csv_data = csv_data.fillna('')
    csv_data['title'] = csv_data['title'].str.lower()
    for i, task in enumerate(task_batch):
        if i % 10 == 0:
            print('thread', threading.currentThread().getName())
            print('processing the {}th out of {}'.format(i, len(task_batch)))
        col = mongo_db[task['col_name']]

        # get doc
        doc = col.find_one({'_id': task['_id']})
        if doc is None:
            continue

        doc_updated = False

        # get metadata
        metadata = None
        if ('metadata' in doc):
            metadata = doc['metadata']

        # get title
        title = None
        raw_title = None
        if metadata is not None:
            if ('title' in metadata
                and isinstance(metadata['title'], str)
                and len(metadata['title'].strip()) > 0
            ):
                raw_title = metadata['title']
                # print('raw_title', raw_title)
                title = clean_title(raw_title)

        # get author
        author_names = None
        if metadata is not None:
            author_names = metadata.get('authors')
            if not (isinstance(author_names, list) and len(author_names) > 0):
                author_names = None

        # get abstract
        abstract = None
        if 'abstract' in doc and len(doc['abstract']) > 0:
            abstract = ''
            for fragment in doc['abstract']:
                if ('text' in fragment
                    and isinstance(fragment['text'], str)
                    and len(fragment['text']) > 0
                ):
                    abstract += fragment['text'].strip() + ' '

            abstract = abstract.strip()
            if len(abstract) == 0:
                abstract = None

        # query csv_data
        matched_item = None
        matched_candidates = []

        # match by title
        if title is not None and matched_item is None:
            similarity = csv_data.apply(
                lambda x: text_similarity_by_char(x['title'], title, quick_mode=True),
                axis=1
            )
            sim_csv_data = csv_data[similarity>=0.5]
            if len(sim_csv_data) > 0:
                similarity = sim_csv_data.apply(
                    lambda x: text_similarity_by_char(x['title'], title, quick_mode=False),
                    axis=1
                )
                sorted_similarity = similarity.sort_values(ascending=False)
                sorted_data = sim_csv_data.reindex(index=sorted_similarity.index)

                print('raw_title: ', raw_title)
                print('title', title)
                print("csv_title", sorted_data.iloc[0]['title'])
                print('similarity', sorted_similarity.iloc[0])
                print(sorted_similarity.head(10))
                print('len(raw_title)', len(raw_title))
                print('doi', sorted_data.iloc[0]['doi'])
                print()

                if (len(title) > LEAST_TITLE_LEN
                    and len(sorted_data.iloc[0]['title']) > LEAST_TITLE_LEN
                    and sorted_similarity.iloc[0] > LEAST_TITLE_SIMILARITY):
                    matched_item = correct_pd_dict(sorted_data.iloc[0].to_dict())

            if matched_item is None and len(sim_csv_data) > 0:
                similarity = sim_csv_data.apply(
                    lambda x: text_similarity_by_char(
                        x['title'],
                        title,
                        quick_mode=False,
                        enable_ignore_begin_end=True,
                        ignore_begin_end_text_len=FIVE_PERCENT_TITLE_LEN,
                        ignore_begin_end_similarity=IGNORE_BEGIN_END_TITLE_SIMILARITY,
                    ),
                    axis=1
                )
                sorted_similarity = similarity.sort_values(ascending=False)
                sorted_data = sim_csv_data.reindex(index=sorted_similarity.index)
                if (len(title) > LEAST_TITLE_LEN
                    and len(sorted_data.iloc[0]['title']) > LEAST_TITLE_LEN
                    and sorted_similarity.iloc[0] > LEAST_TITLE_SIMILARITY
                ):
                    print('result after ignore_begin_end')
                    print('raw_title: ', raw_title)
                    print('title', title)
                    print("csv_title", sorted_data.iloc[0]['title'])
                    print('similarity', sorted_similarity.iloc[0])
                    print()
                    matched_item = correct_pd_dict(sorted_data.iloc[0].to_dict())
                elif (len(title) > LEAST_TITLE_LEN
                    and len(sorted_data.iloc[0]['title']) > LEAST_TITLE_LEN
                    and sorted_similarity.iloc[0] > 0.5
                ):
                    matched_candidates.insert(0, correct_pd_dict(sorted_data.iloc[0].to_dict()))

        if abstract is not None and matched_item is None:
            # match by abstract
            similarity = csv_data.apply(
                lambda x: text_similarity_by_char(x['abstract'], abstract, quick_mode=True),
                axis=1
            )
            sim_csv_data = csv_data[similarity>=0.5]
            if len(sim_csv_data) > 0:
                similarity = sim_csv_data.apply(
                    lambda x: text_similarity_by_char(x['abstract'], abstract, quick_mode=False),
                    axis=1
                )
                sorted_similarity = similarity.sort_values(ascending=False)
                sorted_data = sim_csv_data.reindex(index=sorted_similarity.index)

                print('abstract', abstract)
                print("csv_abstract", sorted_data.iloc[0]['abstract'])
                print('similarity', sorted_similarity.iloc[0])
                print()

                if (len(abstract) > LEAST_ABS_LEN
                    and len(sorted_data.iloc[0]['abstract']) > LEAST_ABS_LEN
                    and sorted_similarity.iloc[0] > LEAST_ABS_SIMILARITY
                ):
                    matched_item = correct_pd_dict(sorted_data.iloc[0].to_dict())
                elif (len(abstract) > LEAST_ABS_LEN
                    and len(sorted_data.iloc[0]['abstract']) > LEAST_ABS_LEN
                    and sorted_similarity.iloc[0] > 0.5
                ):
                    matched_candidates.insert(0, correct_pd_dict(sorted_data.iloc[0].to_dict()))


            if matched_item is None and len(sim_csv_data) > 0:
                similarity = sim_csv_data.apply(
                    lambda x: text_similarity_by_char(
                        x['abstract'],
                        abstract,
                        quick_mode=False,
                        enable_ignore_begin_end=True,
                        ignore_begin_end_text_len=FIVE_PERCENT_ABS_LEN,
                        ignore_begin_end_similarity=IGNORE_BEGIN_END_ABS_SIMILARITY,
                    ),
                    axis=1
                )
                sorted_similarity = similarity.sort_values(ascending=False)
                sorted_data = sim_csv_data.reindex(index=sorted_similarity.index)

                if (len(abstract) > LEAST_ABS_LEN
                    and len(sorted_data.iloc[0]['abstract']) > LEAST_ABS_LEN
                    and sorted_similarity.iloc[0] > LEAST_ABS_SIMILARITY
                ):
                    print('result after ignore_begin_end')
                    print('abstract', abstract)
                    print("csv_abstract", sorted_data.iloc[0]['abstract'])
                    print('similarity', sorted_similarity.iloc[0])
                    print()
                    matched_item = correct_pd_dict(sorted_data.iloc[0].to_dict())


        if (matched_item is None and len(matched_candidates) > 0 and author_names is not None):
            # match by author
            for candidate in matched_candidates:
                if not candidate['authors']:
                    continue
                names_parsed = parse_names(candidate['authors'])
                name_cmp_result = compare_author_names(author_names, names_parsed)
                print('raw_title: ', raw_title)
                print("candidate['title']", candidate['title'])
                print('abstract', abstract)
                print("candidate['abstract']", candidate['abstract'])
                print('author_names', [{'first': x['first'], 'last': x['last']} for x in author_names])
                print("candidate['authors']", candidate['authors'])
                print('name_cmp_result', name_cmp_result)
                print()
                if name_cmp_result:
                    matched_item = candidate
                    break

        if matched_item is None and len(matched_candidates) == 0:
            print('no similar and no candidates!')
            print('raw_title: ', raw_title)
            print('abstract', abstract)
            if author_names:
                print('author_names', [{'first': x['first'], 'last': x['last']} for x in author_names])
            else:
                print('author_names', author_names)
            print()

        # update db
        set_params = {
            "tried_csv_doi": True,
            'last_updated': datetime.now(),
        }

        # update doi found
        if matched_item is not None:
            print("FOUND")
            print()
            set_params['csv_raw_result'] = matched_item
            if (matched_item.get('doi')
                and isinstance(matched_item['doi'], str)
                and len(matched_item['doi'].strip())>0
            ):
                set_params['doi'] = matched_item['doi'].strip()

            doc_updated = True

        try:
            col.find_one_and_update(
                {"_id": doc['_id']},
                {
                    "$set": set_params,
                }
            )
        except Exception as e:
            print('matched_item')
            pprint(matched_item)
            print(e)
            raise e

def foo(mongo_db, num_cores=4):
    for col_name in mongo_db.collection_names():
        # if col_name != 'CORD_noncomm_use_subset':
        #     continue
        if col_name not in PAPER_COLLECTIONS:
            continue
        print('col_name', col_name)
        col = mongo_db[col_name]
        query = col.find(
            {
                "tried_crossref_doi" : { "$exists" : True },
                "doi" : { "$exists" : False },
                "crossref_raw_result": {"$exists": False},
            },
            {
                '_id': True
            }
        )
        # # used for cluster to avoid duplication
        # query = col.aggregate(
        #     [
        #         {
        #             '$match': {
        #                 "tried_crossref_doi": {"$exists": True},
        #                 "doi": {"$exists": False},
        #                 "crossref_raw_result": {"$exists": False},
        #             }
        #         },
        #         {
        #             '$sample': {'size': int(query.count()/4)}
        #         },
        #         {
        #             '$project': {'_id': True}
        #         },
        #     ],
        #     allowDiskUse=True
        # )
        all_tasks = list(query)
        for task in all_tasks:
            task['col_name'] = col_name
        print('len(all_tasks)', len(all_tasks))

        parallel_arguments = []
        num_task_per_batch = int(len(all_tasks) / num_cores)
        print('num_task_per_batch', num_task_per_batch)
        for i in range(num_cores):
            if i < num_cores - 1:
                parallel_arguments.append((all_tasks[i * num_task_per_batch: (i + 1) * num_task_per_batch],))
            else:
                parallel_arguments.append((all_tasks[i * num_task_per_batch:], ))

        p = mp.Pool(processes=num_cores)
        all_summary = p.starmap(doi_match_a_batch_by_crossref, parallel_arguments)
        p.close()
        p.join()


def bar(mongo_db, num_cores=1):
    for col_name in mongo_db.collection_names():
        # if col_name != 'CORD_custom_license':
        #     continue
        if col_name not in PAPER_COLLECTIONS:
            continue
        print('col_name', col_name)
        col = mongo_db[col_name]
        query = col.find(
            {
                "tried_csv_doi": {"$exists": True},
                "doi": {"$exists": False},
                "csv_raw_result": {"$exists": False},
            },
            {
                '_id': True
            }
        )
        # used for cluster to avoid duplication
        query = col.aggregate(
            [
                {
                    '$match': {
                        "tried_csv_doi" : { "$exists" : True },
                        "doi" : { "$exists" : False },
                        "csv_raw_result" : { "$exists" : False },
                    }
                },
                {
                    '$sample': {'size': int(query.count()/4)}
                },
                {
                    '$project': {'_id': True}
                },
            ],
            allowDiskUse=True
        )
        all_tasks = list(query)
        for task in all_tasks:
            task['col_name'] = col_name
        print('len(all_tasks)', len(all_tasks))

        parallel_arguments = []
        num_task_per_batch = int(len(all_tasks) / num_cores)
        print('num_task_per_batch', num_task_per_batch)
        for i in range(num_cores):
            if i < num_cores - 1:
                parallel_arguments.append((all_tasks[i * num_task_per_batch: (i + 1) * num_task_per_batch],))
            else:
                parallel_arguments.append((all_tasks[i * num_task_per_batch:], ))

        p = mp.Pool(processes=num_cores)
        all_summary = p.starmap(doi_match_a_batch_by_csv, parallel_arguments)
        p.close()
        p.join()

def misaka(mongo_db, num_cores=1):
    for col_name in mongo_db.collection_names():
        # if col_name != 'CORD_custom_license':
        #     continue
        if col_name not in PAPER_COLLECTIONS:
            continue
        print('col_name', col_name)
        col = mongo_db[col_name]
        query = col.find(
            {
                # "tried_csv_doi": {"$exists": True},
                # "doi": {"$exists": False},
                "csv_raw_result": {"$exists": False},
            },
            {
                '_id': True
            }
        )
        # used
        all_tasks = list(query)
        for task in all_tasks:
            task['col_name'] = col_name
        print('len(all_tasks)', len(all_tasks))
        print(all_tasks)

        parallel_arguments = []
        num_task_per_batch = int(len(all_tasks) / num_cores)
        print('num_task_per_batch', num_task_per_batch)
        for i in range(num_cores):
            if i < num_cores - 1:
                parallel_arguments.append((all_tasks[i * num_task_per_batch: (i + 1) * num_task_per_batch],))
            else:
                parallel_arguments.append((all_tasks[i * num_task_per_batch:], ))

        p = mp.Pool(processes=num_cores)
        all_summary = p.starmap(doi_match_a_batch_by_csv_new, parallel_arguments)
        p.close()
        p.join()


if __name__ == '__main__':
    db = get_mongo_db('../config.json')
    print(db.collection_names())

    # doi_existence_stat(db)

    # check_doc_wo_doi(db)

    # add_useful_fields(db)

    assign_suggested_doi(db)

    # foo(db, num_cores=4)

    # bar(db, num_cores=4)

    # misaka(db, num_cores=4)

