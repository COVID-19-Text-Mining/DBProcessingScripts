import os
import re
import traceback
from io import BytesIO

import gridfs

from pdf_extractor.paragraphs import extract_paragraphs_pdf
from utils import clean_title


def parse_biorxiv_doc(doc, db):
    parsed_doc = dict()
    parsed_doc['title'] = clean_title(doc['Title'])
    parsed_doc['doi'] = doc['Doi']
    parsed_doc['origin'] = "Scraper_chemrxiv_org"
    parsed_doc['link'] = doc['Link']
    parsed_doc['journal'] = doc['Journal']
    parsed_doc['publication_date'] = doc['Publication_Date']
    parsed_doc['authors'] = doc["Authors"]
    parsed_doc['abstract'] = list(map(lambda x: re.sub(r'\s+', ' ', x), doc['Abstract']))
    parsed_doc['has_year'] = True
    parsed_doc['has_month'] = True
    parsed_doc['has_day'] = True

    paper_fs = gridfs.GridFS(
        db, collection='Scraper_chemrxiv_org_fs')
    pdf_file = paper_fs.get(doc['PDF_gridfs_id'])

    try:
        paragraphs = extract_paragraphs_pdf(BytesIO(pdf_file.read()))
    except Exception as e:
        print('Failed to extract PDF %s(%r) (%r)' % (doc['Doi'], doc['PDF_gridfs_id'], e))
        traceback.print_exc()
        paragraphs = []

    parsed_doc['body_text'] = [{
        'section_heading': None,
        'text': x
    } for x in paragraphs]

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
    collection = db['Scraper_chemrxiv_org']

    for doc in collection.find():
        ret = parse_biorxiv_doc(doc, db)
        pprint(ret)

        if input('Continue? (y/n)').lower() == 'n':
            break
