import datetime
from pprint import pprint

from IndependentScripts.common_utils import get_mongo_db
from parse_cord_papers import parse_cord_doc


PAPER_COLLECTIONS = {
    'CORD_biorxiv_medrxiv',
    'CORD_comm_use_subset',
    'CORD_custom_license',
    'CORD_noncomm_use_subset',
}

def convert_to_vespa_doc(doc,
                         col_name,
                         dataset_version=str(int(datetime.datetime.now().timestamp()))):
    """
    schema source:
    https://github.com/vespa-engine/sample-apps/blob/master/vespa-cloud/
    cord-19-search/src/main/application/schemas/doc.sd

    :param doc:
    :param col_name:
    :return:
    """
    parsed_doc = parse_cord_doc(doc, col_name)

    idx = doc["_id"]

    title = parsed_doc.get('title', '')

    abstract = parsed_doc.get('abstract', '')

    if ('csv_raw_result' in doc
        and isinstance(doc['csv_raw_result'], dict)
        and 'source_x' in doc['csv_raw_result']
        and isinstance(doc['csv_raw_result']['source_x'], str)
        and len(doc['csv_raw_result']['source_x']) > 0
    ):
        source = doc['csv_raw_result']['source_x']
    else:
        source = None

    if ('csv_raw_result' in doc
        and isinstance(doc['csv_raw_result'], dict)
        and 'license' in doc['csv_raw_result']
        and isinstance(doc['csv_raw_result']['license'], str)
        and len(doc['csv_raw_result']['license']) > 0
    ):
        license = doc['csv_raw_result']['license']
    else:
        license = None

    # TODO: should we use none or "" when journal is not available?
    #   currently parsed_doc journal is none when not available
    journal = parsed_doc.get('journal', None)

    url = parsed_doc.get('link', None)

    if ('csv_raw_result' in doc
        and isinstance(doc['csv_raw_result'], dict)
        and 'cord_uid' in doc['csv_raw_result']
        and isinstance(doc['csv_raw_result']['cord_uid'], str)
        and len(doc['csv_raw_result']['cord_uid']) > 0
    ):
        cord_uid = doc['csv_raw_result']['cord_uid']
    else:
        cord_uid = None

    if ('pmcid' in doc
        and isinstance(doc['pmcid'], str)
        and len(doc['pmcid']) > 0
    ):
        pmcid = doc['pmcid']
    else:
        pmcid = None

    if ('pubmed_id' in doc
        and isinstance(doc['pubmed_id'], str)
        and len(doc['pubmed_id']) > 0
    ):
        try:
            pubmed_id = int(doc['pubmed_id'])
        except:
            pubmed_id = None
    else:
        pubmed_id = None

    if ('csv_raw_result' in doc
        and isinstance(doc['csv_raw_result'], dict)
        and 'WHO #Covidence' in doc['csv_raw_result']
        and isinstance(doc['csv_raw_result']['WHO #Covidence'], str)
        and len(doc['csv_raw_result']['WHO #Covidence']) > 0
    ):
        who_covidence = doc['csv_raw_result']['WHO #Covidence']
    else:
        who_covidence = None

    # TODO: how to format this datestring?
    # Since we don't has_year and has_month
    # I think it would be better to use xxxx-xx and xxxx-xx-xx
    # put None if year is not available
    datestring = None
    timestamp = 0
    publication_date = parsed_doc.get('publication_date', None)
    if publication_date is not None:
        datestring = publication_date.strftime('%Y-%m-%d')
        timestamp = int(publication_date.timestamp())

    doi = parsed_doc.get('doi', '')

    if ('csv_raw_result' in doc
        and isinstance(doc['csv_raw_result'], dict)
        and 'has_full_text' in doc['csv_raw_result']
        and isinstance(doc['csv_raw_result']['has_full_text'], bool)
    ):
        has_full_text = doc['csv_raw_result']['has_full_text']
    else:
        has_full_text = None

    authors = []
    # stolen from parse_cord_papers.py and add fields first, middle, last, suffix
    # TODO: should we and an placehold if one field is empty or just ignore that field?
    for a in doc['metadata']['authors']:
        author = dict()
        name = ""
        if a['first'] and a['first'] != "":
            author['first'] = a['first']
            name += a['first']
        if len(a['middle']) > 0:
            author['middle'] = " ".join([m for m in a['middle']])
            name += " " + author['middle']
        if a['last'] and a['last'] != "":
            author['last'] = a['last']
            name += " " + a['last']
        if a['suffix'] and a['suffix'] != "":
            author['suffix'] = a['suffix']
            name += " " + a['suffix']
        author['name'] = name

        if a['email'] != "":
            author['email'] = a['email'].strip()

        if len(author['name']) > 3:
            authors.append(author)

    if len(authors) == 0:
        authors = None

    bib_entries = []
    if ('bib_entries' in doc
        and isinstance(doc['bib_entries'], dict)
        and len(doc['bib_entries']) > 0
    ):
        for bib in doc['bib_entries'].values():
            bib_entries.append({
                'ref_id': bib.get('ref_id', ''),
                'title': bib.get('title', ''),
                'year': bib.get('year', None),
                'issn': str(bib.get('issn', '')),
            })
    if len(bib_entries) == 0:
        bib_entries = None

    body_text = parsed_doc.get('body_text', None)
    if body_text is not None:
        body_text = '\n'.join([x['Text'].strip() for x in body_text])

    conclusion = None
    results = None
    discussion = None
    methods = None
    background = None
    introduction = None

    # TODO: is this necessary? Vespa script has this. But we need to confirm.
    if doi:
        doi = 'https://doi.org/%s' % doi

    vespa_doc = {
        'title': title,
        '_id': idx,
        'source': source,
        'license': license,
        'datestring': datestring,
        'doi': doi,
        'url': url,
        'cord_uid': cord_uid,
        'authors': authors,
        'bib_entries': bib_entries,
        'abstract': abstract,
        'journal': journal,
        'body_text': body_text,
        'conclusion': conclusion,
        'introduction': introduction,
        'results': results,
        'discussion': discussion,
        'methods': methods,
        'background': background,
        'timestamp': timestamp,
        'pmcid': pmcid,
        'pubmed_id': pubmed_id,
        'who_covidence': who_covidence,
        'has_full_text': has_full_text,
        'dataset_version': dataset_version,
    }
    return vespa_doc


def parse_cord_for_vespa(mongo_db):
    dataset_version = str(int(datetime.datetime.now().timestamp()))
    for col_name in mongo_db.collection_names():
        if col_name not in PAPER_COLLECTIONS:
            continue
        col = mongo_db[col_name]
        vespa_col = mongo_db['Vespa_{}_parsed'.format(col_name)]

        query = col.find({})
        total_num = query.count()
        for i, doc in enumerate(query):
            if i%1000 == 0:
                print('{} out of {}'.format(i, total_num))
            if not ('doi' in doc and isinstance(doc['doi'], str)):
                continue
            vespa_doc = convert_to_vespa_doc(doc, col_name, dataset_version=dataset_version)
            vespa_col.find_one_and_update(
                {'_id': vespa_doc['_id']},
                {
                    '$set': vespa_doc,
                },
                upsert=True,
            )


if __name__ == '__main__':
    db = get_mongo_db('../config.json')
    print(db.collection_names())

    parse_cord_for_vespa(db)
