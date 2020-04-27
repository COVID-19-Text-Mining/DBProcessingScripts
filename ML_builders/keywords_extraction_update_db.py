import json
from pprint import pprint

from IndependentScripts.common_utils import get_mongo_db
from keywords_extraction import KeywordsExtractorBase
from keywords_extraction import KeywordsExtractorNN

def extract_keywords_in_entries(mongo_db):

    processed_ids = set()

    extractor = KeywordsExtractorNN(
        only_extractive=True,
        use_longest_phrase=True,
    )

    col_name = 'entries'
    col = mongo_db[col_name]
    query = col.find(
        {
            "doi": {"$exists": True},
            "abstract": {"$exists": True},
        },
        {
            '_id': True,
            'doi': True,
            'abstract': True,
        },
        no_cursor_timeout=True
    )
    total_num = query.count()
    print('query.count()', total_num)
    for i, doc in enumerate(query):
        if i%1000 == 0:
            print('extract_keywords_in_entries: {} out {}'.format(i, total_num))
        if str(doc['_id']) in processed_ids:
            continue
        if doc['abstract']:
            abstract = KeywordsExtractorBase().clean_html_tag(doc['abstract'])
            try:
                keywords = extractor.process(abstract)
            except:
                print('Error')
                print('doi:', doc.get('doi'))
                print(abstract)
            # update in db
            col.find_one_and_update(
                {"_id": doc['_id']},
                {
                    "$set": {
                        'keywords_ML': keywords,
                    },
                }
            )
            processed_ids.add(str(doc['_id']))
            if len(processed_ids) % 1000 == 0:
                with open('../scratch/processed_ids.json', 'w') as fw:
                    json.dump(list(processed_ids), fw, indent=2)

if __name__ == '__main__':
    db = get_mongo_db('../config.json')
    print(db.collection_names())

    extract_keywords_in_entries(db)