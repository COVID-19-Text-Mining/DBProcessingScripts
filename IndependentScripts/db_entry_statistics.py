import json

from pprint import pprint
import collections
from bson import json_util, objectid

from common_utils import get_mongo_db

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

def entry_collection_stat(mongo_db):
    stat_counter = collections.Counter()
    CORD_id = []
    entries_CORD_doi = []
    entries_all_doi = []
    for col_name in mongo_db.collection_names():
        if col_name not in PAPER_COLLECTIONS:
            continue
        col = mongo_db[col_name]
        query_w_doi = col.find(
            {'doi': {'$exists': True}},
            {'doi': True},
        )
        query_w_suggest_id = col.find(
            {'suggested_id': {'$exists': True}},
            {
                '_id': True,
                'suggested_id': True
            },
        )
        query_w_title = col.find({
            'metadata': {'$exists': True},
            '$where': 'this.metadata.title.length > 0',
        })
        query_w_abstract = col.find({'abstract.0': {'$exists': True}})
        query_w_title_abs = col.find({
            'metadata': {'$exists': True},
            '$where': 'this.metadata.title.length > 0',
            'abstract.0': {'$exists': True},
        })
        query_w_body = col.find({'body_text.0': {'$exists': True}})

        for doc in query_w_suggest_id:
            CORD_id.append({
                '_id': doc['_id'],
                'suggested_id':doc['suggested_id'],
                'source': col_name,
            })

        stat_counter['col_{}_all'.format(col.name)] = col.count()
        stat_counter['col_{}_w_suggest_id'.format(col.name)] = query_w_suggest_id.count()
        stat_counter['col_{}_w_doi'.format(col.name)] = query_w_doi.count()
        stat_counter['col_{}_w_title'.format(col.name)] = query_w_title.count()
        stat_counter['col_{}_w_abstract'.format(col.name)] = query_w_abstract.count()
        stat_counter['col_{}_w_title_abs'.format(col.name)] = query_w_title_abs.count()
        stat_counter['col_{}_w_body'.format(col.name)] = query_w_body.count()
        stat_counter['CORD_all'] += col.count()
        stat_counter['CORD_all_w_suggest_id'] += query_w_suggest_id.count()
        stat_counter['CORD_all_w_doi'] += query_w_doi.count()
        stat_counter['CORD_all_w_title'] += query_w_title.count()
        stat_counter['CORD_all_w_abstract'] += query_w_abstract.count()
        stat_counter['CORD_all_w_title_abs'] += query_w_title_abs.count()
        stat_counter['CORD_all_w_body'] += query_w_body.count()



    # query all entries
    col = mongo_db['entries']
    query = col.find({})
    query_w_doi = col.find(
        {
            'doi': {'$exists': True},
        },
        {
            'doi': True,
            'origin': True,
        },
    )

    for doc in query_w_doi:
        entries_all_doi.append({
            'doi': doc['doi'],
            'source': doc['origin'],
        })
    stat_counter['entries_all'] = query.count()
    stat_counter['entries_all_w_doi'] = query_w_doi.count()

    # query entries in PAPER_COLLECTIONS
    col = mongo_db['entries']
    query = col.find({'origin': {'$in': list(PAPER_COLLECTIONS)}})
    query_w_doi = col.find(
        {
            'origin': {'$in': list(PAPER_COLLECTIONS)},
            'doi': {'$exists': True},
        },
        {
            'doi': True,
            'origin': True,
        },
    )

    for doc in query_w_doi:
        entries_CORD_doi.append({
            'doi': doc['doi'],
            'source': doc['origin'],
        })

    stat_counter['entries_CORD'] = query.count()
    stat_counter['entries_CORD_w_doi'] = query_w_doi.count()


    print('stat_counter')
    pprint(stat_counter)
    print('len(CORD_id)', len(CORD_id))
    print('len(entries_all_doi)', len(entries_all_doi))
    print('len(entries_CORD_doi)', len(entries_CORD_doi))

    with open('../scratch/CORD_id.json', 'w') as fw:
        json.dump(CORD_id, fw, indent=2, default=json_util.default)

    with open('../scratch/entries_CORD_doi.json', 'w') as fw:
        json.dump(entries_CORD_doi, fw, indent=2)

    with open('../scratch/entries_all_doi.json', 'w') as fw:
        json.dump(entries_all_doi, fw, indent=2)

def query_missed_entries(mongo_db):
    stat_counter = collections.Counter()
    stat_counter_in_all = collections.Counter()

    with open('../scratch/CORD_id.json', 'r') as fr:
        CORD_data = json.load(fr)

    with open('../scratch/entries_CORD_doi.json', 'r') as fr:
        entries_CORD_data = json.load(fr)

    with open('../scratch/entries_all_doi.json', 'r') as fr:
        entries_all_data = json.load(fr)

    entries_CORD_doi = set([doc['doi'] for doc in entries_CORD_data])
    entries_all_doi = set([doc['doi'] for doc in entries_all_data])

    CORD_id = set([doc['suggested_id']['id'] for doc in CORD_data])
    missed_doi = CORD_id - entries_CORD_doi
    extra_doi = entries_CORD_doi - CORD_id
    missed_doi_in_all = CORD_id - entries_all_doi

    missed_data = list(filter(lambda x: x['suggested_id']['id'] in missed_doi, CORD_data))
    missed_data_in_all = list(filter(lambda x: x['suggested_id']['id'] in missed_doi_in_all, CORD_data))

    print('len(CORD_id)', len(CORD_id))
    print('len(entries_CORD_doi)', len(entries_CORD_doi))
    print('len(missed_doi)', len(missed_doi))
    print('len(extra_doi)', len(extra_doi))
    print('len(entries_all_doi)', len(entries_all_doi))
    print('len(missed_doi_in_all)', len(missed_doi_in_all))

    CORD_id_non_doi = set([doc['suggested_id']['id'] for doc in CORD_data if doc['suggested_id']['source'] != 'doi'])
    print('len(CORD_id_non_doi&entries_CORD_doi)', len(CORD_id_non_doi&entries_CORD_doi))
    print('len(CORD_id_non_doi&entries_all_doi)', len(CORD_id_non_doi&entries_all_doi))

    for p in missed_data:
        _id = objectid.ObjectId(p['_id']['$oid'])
        col_name = p['source']
        missed_entry = mongo_db[col_name].find_one(
            {
                '_id': _id,
            }
        )
        stat_counter['missed_entry'] += 1
        flags = {}
        if ('doi' in missed_entry
            and isinstance(missed_entry['doi'], str)
            and len(missed_entry['doi']) > 0):
            flags['w_doi'] = True
            stat_counter['w_doi'] += 1
        if ('metadata' in missed_entry
            and isinstance(missed_entry['metadata']['title'], str)
            and len(missed_entry['metadata']['title']) > 0):
            stat_counter['w_title'] += 1
            flags['w_title'] = True
        if ('abstract' in missed_entry
            and isinstance(missed_entry['abstract'], list)
            and len(missed_entry['abstract']) > 0):
            stat_counter['w_abstract'] += 1
            flags['w_abstract'] = True
        if ('body_text' in missed_entry
            and isinstance(missed_entry['body_text'], list)
            and len(missed_entry['body_text']) > 0):
            stat_counter['w_body'] += 1
            flags['w_body'] = True

        if (flags.get('w_doi') == True
            and flags.get('w_title') == True):
            stat_counter['w_doi_title'] += 1

        if (flags.get('w_doi') == True
            and flags.get('w_abstract') == True):
            stat_counter['w_doi_abstract'] += 1

        if (flags.get('w_doi') == True
            and flags.get('w_title') == True
            and flags.get('w_abstract') == True):
            stat_counter['w_doi_title_abstract'] += 1

    for p in missed_data_in_all:
        _id = objectid.ObjectId(p['_id']['$oid'])
        col_name = p['source']
        missed_entry = mongo_db[col_name].find_one(
            {
                '_id': _id,
            }
        )
        stat_counter_in_all['missed_entry'] += 1
        flags = {}
        if ('doi' in missed_entry
            and isinstance(missed_entry['doi'], str)
            and len(missed_entry['doi']) > 0):
            flags['w_doi'] = True
            stat_counter_in_all['w_doi'] += 1
        if ('metadata' in missed_entry
            and isinstance(missed_entry['metadata']['title'], str)
            and len(missed_entry['metadata']['title']) > 0):
            stat_counter_in_all['w_title'] += 1
            flags['w_title'] = True
        if ('abstract' in missed_entry
            and isinstance(missed_entry['abstract'], list)
            and len(missed_entry['abstract']) > 0):
            stat_counter_in_all['w_abstract'] += 1
            flags['w_abstract'] = True
        if ('body_text' in missed_entry
            and isinstance(missed_entry['body_text'], list)
            and len(missed_entry['body_text']) > 0):
            stat_counter_in_all['w_body'] += 1
            flags['w_body'] = True

        if (flags.get('w_doi') == True
            and flags.get('w_title') == True):
            stat_counter_in_all['w_doi_title'] += 1

        if (flags.get('w_doi') == True
            and flags.get('w_abstract') == True):
            stat_counter_in_all['w_doi_abstract'] += 1

        if (flags.get('w_doi') == True
            and flags.get('w_title') == True
            and flags.get('w_abstract') == True):
            stat_counter_in_all['w_doi_title_abstract'] += 1

    print('stat_counter')
    pprint(stat_counter)
    print('stat_counter_in_all')
    pprint(stat_counter_in_all)

def entries_keywords_stat(mongo_db):
    # query entries in PAPER_COLLECTIONS
    col = mongo_db['entries']
    query_w_keywords = col.find({'keywords.0': {'$exists': True}})

    print('query_w_keywords.count()', query_w_keywords.count())
    for doc in query_w_keywords:
        print(doc['doi'], doc['origin'])

if __name__ == '__main__':
    db = get_mongo_db('../config.json')
    print(db.collection_names())
    # entry_collection_stat(db)
    # query_missed_entries(db)

    entries_keywords_stat(db)