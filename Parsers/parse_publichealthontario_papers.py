import os
import re
import traceback
from io import BytesIO

import gridfs

from pdf_extractor.paragraphs import extract_paragraphs_pdf
from utils import clean_title


def expand_bullet(text):
    bullets = re.split(r'\uf0b7', text)
    bullets = list(filter(lambda x: len(x), map(
        lambda x: re.sub(r'\s+', ' ', x.strip()),
        bullets
    )))
    return bullets


def parse_synopsis_doc(doc, db):
    parsed_doc = dict()
    parsed_doc['title'] = clean_title(doc['Title'])
    parsed_doc['link'] = doc['Link']
    parsed_doc['origin'] = "Scraper_public_health_ontario"
    parsed_doc['journal_string'] = doc['Journal_String']
    parsed_doc['authors'] = doc["Authors"]

    paper_fs = gridfs.GridFS(
        db, collection='Scraper_publichealthontario_fs')
    pdf_file = paper_fs.get(doc['PDF_gridfs_id'])

    with open('example.pdf', 'wb') as f:
        f.write(pdf_file.read())
        pdf_file.seek(0)

    try:
        paragraphs = extract_paragraphs_pdf(BytesIO(pdf_file.read()))
    except Exception as e:
        print('Failed to extract PDF %s(%r) (%r)' % (doc['Doi'], doc['PDF_gridfs_id'], e))
        traceback.print_exc()
        paragraphs = []

    text = ' '.join(paragraphs)
    m = re.search(
        r'One-Minute Summary(.*?)'
        r'Additional Information(.*?)'
        r'PHO Reviewer’s Comments(.*?)'
        r'Citation', text)
    if not m:
        parsed_doc['synopsis'] = None
    else:
        parsed_doc['synopsis'] = {
            'summary': expand_bullet(m.group(1)),
            'additional_info': expand_bullet(m.group(2)),
            'pho_reviewer_comments': expand_bullet(m.group(3)),
        }

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

    for doc in collection.find():
        ret = parse_synopsis_doc(doc, db)
        pprint(ret)

        for p in ret['synopsis']:
            print(p)
            print('-' * 70)
            for i in ret['synopsis'][p]:
                print(' -', i)

        if input('Continue? (y/n)').lower() == 'n':
            break
