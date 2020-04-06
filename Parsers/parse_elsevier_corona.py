import json
import os
import re
from datetime import datetime

from LimeSoup.ElsevierSoup_XML import ElsevierXMLSoup

from utils import clean_title


def parse_elsevier_doc(doc, db):
    meta_col = db['Elsevier_corona_meta']

    paper_meta = meta_col.find_one({'paper_id': doc['paper_id']})
    assert paper_meta is not None
    paper_meta = json.loads(paper_meta['meta'])

    parsed_paper = ElsevierXMLSoup.parse(doc['xml'])

    parsed_doc = {
        'doi': paper_meta["full-text-retrieval-response"]["coredata"]["prism:doi"],
        'title': paper_meta["full-text-retrieval-response"]["coredata"]["dc:title"],
        'origin': "Scraper_Elsevier_corona",
        'link': None,
        'journal': paper_meta["full-text-retrieval-response"]["coredata"]["prism:publicationName"],
        'abstract': paper_meta["full-text-retrieval-response"]["coredata"]["dc:description"],
        'publication_date': datetime.strptime(
            paper_meta["full-text-retrieval-response"]["coredata"]["prism:coverDate"],
            '%Y-%m-%d'),
        'has_year': True,
        'has_month': True,
        'has_day': True,
    }

    for i in paper_meta["full-text-retrieval-response"]["coredata"].get("link", ()):
        if i["@rel"] == "scidir":
            parsed_doc['link'] = i['@href']
            break
    if parsed_doc['link'] is None:
        parsed_doc['link'] = 'https://doi.org/' + parsed_paper['doi']

    # Fix abstract
    parsed_doc['abstract'] = parsed_doc['abstract'] or ''
    parsed_doc['abstract'] = re.sub(r'\s+', ' ', parsed_doc['abstract']).strip()
    parsed_doc['abstract'] = re.sub(r'^abstract\s+', '', parsed_doc['abstract'],
                                    flags=re.IGNORECASE)

    parsed_doc['title'] = clean_title(parsed_doc['title'])

    parsed_doc['authors'] = []
    for i in paper_meta["full-text-retrieval-response"]["coredata"].get('dc:creator', ()):
        name = i['$']
        if ',' in name:
            name = ' '.join(map(lambda x: x.strip(), reversed(name.split(','))))
        parsed_doc['authors'].append({'name': name})

    # print(parsed_paper)
    # Use None for now, TODO: fix it later
    parsed_doc['body_text'] = None
    # parsed_doc['body_text'] = [{
    #     'section_heading': None,
    #     'text': x
    # } for x in paragraphs]

    #Needed for builder to be happy
    parsed_doc['mtime'] = doc['mtime']
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
    collection = db['Elsevier_corona_xml']

    for doc in collection.find():
        ret = parse_elsevier_doc(doc, db)
        pprint(ret)

        if input('Continue? (y/n)').lower() == 'n':
            break
