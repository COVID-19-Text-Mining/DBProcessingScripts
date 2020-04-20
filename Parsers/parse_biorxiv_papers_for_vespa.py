import re
import traceback
from datetime import datetime
from io import BytesIO

import gridfs
from tqdm import tqdm

from pdf_extractor.paragraphs import extract_paragraphs_pdf
from utils import clean_title


def try_parse_pdf_hierarchy(pdf_file):
    try:
        paragraphs = extract_paragraphs_pdf(BytesIO(pdf_file.read()))
    except Exception as e:
        print('Failed to extract PDF %s(%r) (%r)' % (doc['Doi'], doc['PDF_gridfs_id'], e))
        traceback.print_exc()
        paragraphs = []

    headings = r'\n' \
               r'(?:abstract|backgrounds?|introduction|methods?|' \
               r'results?|discussions?|conclusions?|acknowledgements?|' \
               r'references?)'
    continuing_section = fr'(?:.(?!{headings}))'
    sections = fr"""
    (?:
    (?:^|\n)\s*
    (?:
        abstract\s+(?P<abstract>{continuing_section}+)*|
        backgrounds?\s+(?P<background>{continuing_section}+)|
        introduction\s+(?P<introduction>{continuing_section}+)|
        methods?\s+(?P<method>{continuing_section}+)|
        results?\s+(?P<result>{continuing_section}+)|
        discussions?\s+(?P<discussion>{continuing_section}+)|
        conclusions?\s+(?P<conclusion>{continuing_section}+)|
        acknowledgements??\s+(?P<acknowledgement>{continuing_section}+)|
        references?\s+(?P<reference>{continuing_section}+)
    )
    )+
    """
    sections = re.compile(sections, re.VERBOSE | re.DOTALL | re.IGNORECASE)

    body_text = '\n'.join(paragraphs)
    parsed_content = {'body': body_text}
    for match in re.finditer(sections, body_text):
        for name, value in match.groupdict().items():
            if value is not None:
                parsed_content[name] = value

    # print(body_text)

    return parsed_content


def convert_biorxiv_to_vespa(doc, db):
    # paper_fs = gridfs.GridFS(
    #     db, collection='Scraper_connect_biorxiv_org_fs')
    # pdf_file = paper_fs.get(doc['PDF_gridfs_id'])

    # parsed_content = try_parse_pdf_hierarchy(pdf_file)
    parsed_content = {}

    parsed_doc = {
        'title': clean_title(doc['Title']),
        '_id': doc['_id'],
        'source': doc['Journal'],
        'license': doc['Journal'],
        'datestring': doc['Publication_Date'].strftime('%Y-%m-%d'),
        'doi': doc['Doi'],
        'url': doc['Link'],
        'cord_uid': None,
        'authors': [],
        'bib_entries': None,
        'abstract': ' '.join(doc['Abstract']),
        'journal': doc['Journal'],
        'body_text': parsed_content.get('body', None),
        'conclusion': parsed_content.get('conclusion', None),
        'introduction': parsed_content.get('introduction', None),
        'results': parsed_content.get('result', None),
        'discussion': parsed_content.get('discussion', None),
        'methods': parsed_content.get('method', None),
        'background': parsed_content.get('background', None),
        'timestamp': int(doc['Publication_Date'].timestamp()),
        'pmcid': None,
        'pubmed_id': None,
        'who_covidence': None,
        'has_full_text': len(parsed_content.get('body', '')) > 0,
        'dataset_version': datetime.now().timestamp(),
    }

    for person in doc["Authors"]:
        parsed_doc['authors'].append({
            'first': person['Name']['fn'],
            'last': person['Name']['ln'],
            'name': f'{person["Name"]["fn"]} {person["Name"]["ln"]}'
        })

    return parsed_doc


if __name__ == '__main__':
    import os
    from pymongo import MongoClient
    from pprint import pprint

    db = MongoClient(
        host=os.environ['MONGO_HOSTNAME'],
    )[os.environ['MONGO_DB']]
    db.authenticate(
        name=os.environ['MONGO_USERNAME'],
        password=os.environ['MONGO_PASSWORD'],
        source=os.environ['MONGO_AUTHENTICATION_DB']
    )
    collection = db['Scraper_connect_biorxiv_org']
    target_collection = db['Vespa_biorxiv_medrxiv_parsed']

    new_documents = collection.aggregate([
        {'$lookup': {
            'from': 'Vespa_biorxiv_medrxiv_parsed',
            'localField': '_id',
            'foreignField': '_id',
            'as': 'parsed'
        }},
        {'$addFields': {
                '_last_updated': {'$subtract': ["$last_updated", datetime(year=1970, month=1, day=1)]},
                'last_parsed': {'$arrayElemAt': ['$parsed', 0]}
        }},
        {'$addFields': {
            'last_parsed': {'$multiply': ['$last_parsed.dataset_version', 1000]}
        }},
        {'$addFields': {
            'should_update':
                {'$cmp': ['$_last_updated', '$last_parsed']}
        }},
        {'$match': {
            'should_update': 1
        }}
    ])

    for doc in tqdm(new_documents):
        ret = convert_biorxiv_to_vespa(doc, db)
        target_collection.insert(ret)
