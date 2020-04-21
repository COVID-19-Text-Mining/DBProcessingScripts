import traceback
from io import BytesIO

import gridfs

from pdf_extractor.paragraphs import extract_paragraphs_pdf
from utils import clean_title


def parse_biorxiv_doc(doc, db):
    parsed_doc = dict()
    parsed_doc['title'] = clean_title(doc['Title'])
    parsed_doc['doi'] = doc['Doi']
    parsed_doc['origin'] = "Scraper_connect_biorxiv_org"
    parsed_doc['link'] = doc['Link']
    parsed_doc['journal'] = doc['Journal']
    parsed_doc['publication_date'] = doc['Publication_Date']

    author_list = doc["Authors"]
    for a in author_list:
        a['Name'] = a['Name']['fn'] + " " + a['Name']['ln']

    parsed_doc['authors'] = author_list
    parsed_doc['abstract'] = ' '.join(doc['Abstract'])
    parsed_doc['has_year'] = True
    parsed_doc['has_month'] = True
    parsed_doc['has_day'] = True

    # paper_fs = gridfs.GridFS(
    #     db, collection='Scraper_connect_biorxiv_org_fs')
    # pdf_file = paper_fs.get(doc['PDF_gridfs_id'])

    # try:
    #     paragraphs = extract_paragraphs_pdf(BytesIO(pdf_file.read()))
    # except Exception as e:
    #     print('Failed to extract PDF %s(%r) (%r)' % (doc['Doi'], doc['PDF_gridfs_id'], e))
    #     traceback.print_exc()
    #     paragraphs = []

    # parsed_doc['body_text'] = [{
    #     'section_heading': None,
    #     'text': x
    # } for x in paragraphs]

    return parsed_doc
