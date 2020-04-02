import os
import re
import traceback
from io import BytesIO

import gridfs

from pdf_extractor.paragraphs import extract_paragraphs_pdf
from utils import clean_title


def expand_bullet(text):
    bullets = re.split(r'\n', text)
    bullets = list(filter(lambda x: len(x), map(
        lambda x: re.sub(r'\s+', ' ', x.strip()),
        bullets
    )))
    return bullets


def find_fn(l, key):
    for i, j in enumerate(l):
        if key(j):
            return i
    return -1


def find_abstract(abs_list):
    if abs_list is None:
        return None

    abs_list = list(filter(lambda x: x.count(' ') > 5, abs_list))
    if len(abs_list) < 10:
        return ' '.join(abs_list)
    else:
        return abs_list[0]


def parse_synopsis_doc(doc, db):
    parsed_doc = dict()
    parsed_doc['title'] = clean_title(doc['Title'])
    parsed_doc['link'] = doc['Link']
    parsed_doc['origin'] = "Scraper_public_health_ontario"
    parsed_doc['journal_string'] = doc['Journal_String'].strip(' \t\r.')
    parsed_doc['authors'] = doc["Authors"]
    parsed_doc['abstract'] = find_abstract(doc.get('Abstract'))

    paper_fs = gridfs.GridFS(
        db, collection='Scraper_publichealthontario_fs')
    pdf_file = paper_fs.get(doc['PDF_gridfs_id'])

    # with open('example.pdf', 'wb') as f:
    #     f.write(pdf_file.read())
    #     pdf_file.seek(0)

    try:
        paragraphs = extract_paragraphs_pdf(
            BytesIO(pdf_file.read()),
            return_dicts=True, only_printable=True)
    except Exception as e:
        print('Failed to extract PDF %s(%r) (%r)' % (doc['Doi'], doc['PDF_gridfs_id'], e))
        traceback.print_exc()
        paragraphs = []

    sections = {}
    last_sec = None
    for p in paragraphs:
        is_heading = 18 < p['bbox'][3] - p['bbox'][1] and p['bbox'][2] - p['bbox'][0] < 200
        if is_heading:
            last_sec = p['text'].lower()
            sections[last_sec] = []
        elif last_sec is not None:
            sections[last_sec].append(p)

    parsed_doc['synopsis'] = {
        'summary': sections.get('one-minute summary', None),
        'additional_info': sections.get('additional information', None),
        'pho_reviewer_comments': sections.get('pho reviewers comments', None),
    }
    if all(x is None for x in parsed_doc['synopsis'].values()):
        parsed_doc['synopsis'] = None

    return parsed_doc


if __name__ == '__main__':
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
    collection = db['Scraper_publichealthontario']
    target_collection = db['Scraper_publichealthontario_parsed']

    perform_update = True
    for doc in collection.find():
        ret = parse_synopsis_doc(doc, db)

        if perform_update:
            target_collection.update({'link': ret['link']}, ret, upsert=True)
        else:
            pprint(ret)

            for p in ret['synopsis']:
                print(p)
                print('-' * 70)
                for i in ret['synopsis'][p]:
                    print(' -', i)

            if input('Continue? (y/n)').lower() == 'n':
                break
