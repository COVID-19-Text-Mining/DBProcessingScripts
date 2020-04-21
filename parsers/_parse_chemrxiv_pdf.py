import datetime
import multiprocessing
import os
import traceback
from io import BytesIO
from multiprocessing.pool import Pool

from pymongo import MongoClient
from tqdm import tqdm
import gridfs
from pdf_extractor.paragraphs import extract_paragraphs_pdf

db = MongoClient(
    host=os.environ['COVID_HOST'],
    connect=False
)
db_connected = False
_collection = None
parser_version = 'chemrxiv_20200421'
laparams = {
    'char_margin': 3.0,
    'line_margin': 2.5
}


def auth_db():
    """Auth database later to avoid connecting before forking"""
    global db_connected, _collection

    if db_connected is False:
        _db = db[os.environ['COVID_DB']]

        _db.authenticate(
            name=os.environ['COVID_USER'],
            password=os.environ['COVID_PASS'],
            source=os.environ['COVID_DB']
        )
        collection = _db['Scraper_chemrxiv_org_fs.files']

        fs = gridfs.GridFS(_db, collection='Scraper_chemrxiv_org_fs')

        _collection = collection, fs

    return _collection


def handle_doc(file_obj):
    collection, fs = auth_db()

    # check again!
    doc = collection.find_one({'_id': file_obj['_id']})
    if 'pdf_extraction_version' in doc and \
            doc['pdf_extraction_version'] == parser_version and \
            'parsed_date' in doc and \
            doc['parsed_date'] > doc['uploadDate']:
        return None, None

    pdf_file = fs.find_one(file_obj['_id'])
    data = BytesIO(pdf_file.read())
    try:
        paragraphs = extract_paragraphs_pdf(data, laparams=laparams, return_dicts=True)
        collection.update(
            {'_id': file_obj['_id']},
            {'$set': {
                'pdf_extraction_success': True,
                'pdf_extraction_plist': paragraphs,
                'pdf_extraction_exec': None,
                'pdf_extraction_version': parser_version,
                'parsed_date': datetime.datetime.now(),
            }})
        exc = None
    except Exception as e:
        paragraphs = None
        traceback.print_exc()
        exc = f'Failed to extract PDF {file_obj["filename"]} {e}' + traceback.format_exc()
        collection.update(
            {'_id': file_obj['_id']},
            {'$set': {
                'pdf_extraction_success': False,
                'pdf_extraction_plist': None,
                'pdf_extraction_exec': exc,
                'pdf_extraction_version': parser_version,
                'parsed_date': datetime.datetime.now(),
            }})

    return paragraphs, exc


def process_documents(processes):
    with Pool(processes=processes) as pool:
        collection, _ = auth_db()
        collection.create_index('parsed_date')
        collection.create_index('uploadDate')
        query = {
            '$or': [
                {'$expr': {'$lt': ['$parsed_date', '$uploadDate']}},
                {'pdf_extraction_version': {'$ne': parser_version}},
            ]

        }

        for paragraphs, exc in tqdm(
                pool.imap_unordered(
                    handle_doc, collection.find(query), chunksize=1),
                total=collection.count_documents(query)
        ):
            pass
            # print(exc)
            # with open('file.pdf', 'wb') as f:
            #     data.seek(0)
            #     f.write(data.read())
            # with open('paragraphs.txt', 'w') as f:
            #     f.write('\n'.join([x['text'] for x in paragraphs]))
            # input()


if __name__ == '__main__':
    process_documents(processes=multiprocessing.cpu_count())
