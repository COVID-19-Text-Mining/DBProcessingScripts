import os
import pymongo
import json
from tqdm import tqdm
import datetime
import unidecode
from multiprocessing import Pool
from bson import ObjectId
client = pymongo.MongoClient(os.getenv("COVID_HOST"), username=os.getenv("COVID_USER"),
                                 password=os.getenv("COVID_PASS"), authSource=os.getenv("COVID_DB"))
db = client[os.getenv("COVID_DB")]

def doc_to_json(doc):
    if not "timestamp" in doc:
        try:
            doc["timestamp"] = int(doc["publication_date"].timestamp())
        except (ValueError, KeyError):
            doc["timestamp"] = int(doc["last_updated"].timestamp())

    if doc.get("publication_date") and doc.get("has_year", False):
        year = int(doc["publication_date"].strftime("%Y"))
        if doc["has_year"] and int(year) < 2019:
            doc["is_covid19"] = False
        if doc["has_year"]:
            doc["year"] = int(doc["publication_date"].strftime("%Y"))

    if "publication_date" in doc:
        doc["datestring"] = doc["publication_date"].strftime("%M/%D/%Y")

    if "cited_by" in doc:
        doc["citations_count_total"] = len(doc.get("cited_by", []))

    if "authors" in doc:
        doc["authors"] = [{"name": a["name"]} for a in doc["authors"] if "name" in a]

    if "body_text" in doc and isinstance(doc['body_text'], list):
        doc["body_text"] = " ".join([x["text"] for x in doc["body_text"]]) if "body_text" in doc else None

    if "is_covid19_ML" in doc:
        doc["is_covid19_ml"] = doc["is_covid19_ML"]
        del doc["is_covid19_ML"]
    else:
        doc["is_covid19_ml"] = 0

    if "keywords_ML" in doc:
        doc["keywords_ml"] = doc["keywords_ML"]
        del doc["keywords_ML"]
    else:
        doc["keywords_ml"] = []

    if "last_twitter_search" in doc:
        del doc["last_twitter_search"]

    if "tweets" in doc:
        del doc['tweets']

    if "altmetric" in doc:
        del doc['altmetric']
    # Tags #
    tags = []
    if "doi" in doc and doc["doi"] is not None:
        possible_match = db.entries_categories_ml.find_one({"doi": doc["doi"]})
        if possible_match:
            possible_tags = possible_match["categories"]
            tags = [key for key in possible_tags if possible_tags[key][0] == True]
    doc["tags"] =  tags

    #TODO: Find way to make ids shorter
    #print(unidecode.unidecode(doc.get('title', "")))
    if doc.get('title',None) is not None:
        doc['title'] = doc['title'].strip('[').strip(']')
    if '_id' in doc.keys():
        doc["id"] = str(doc['_id'])
        del doc["_id"]
    else:
        if 'id' not in doc.keys():
            print('no id')
            return None
    if "publication_date" in doc:
        del doc["publication_date"]
    if "last_updated" in doc:
        del doc["last_updated"]
    if "_bt" in doc:
        del doc["_bt"]
    if "unparsed_document" in doc:
        del doc["unparsed_document"]
    if "_cls" in doc:
        del doc["_cls"]
    if "origin" in doc:
        del doc["origin"]
    if "category_human" in doc:
        del doc["category_human"]
    if "has_year" in doc:
        del doc["has_year"]
    if "has_month" in doc:
        del doc["has_month"]
    if "has_day" in doc:
        del doc["has_day"]
    if "source_documents" in doc:
        del doc["source_documents"]
    abstract_embedding = doc.get("embeddings", {}).get("abstract_embedding", None)
    if abstract_embedding:
        abstract_embedding = {"values": abstract_embedding}
    doc["abstract_embedding"] = abstract_embedding
    title_embedding = doc.get("embeddings", {}).get("title_embedding", None)
    if title_embedding:
        title_embedding = {"values": title_embedding}
    doc["title_embedding"] = title_embedding
    if "embeddings" in doc:
        del doc["embeddings"]
    if "summary_human" in doc:
        doc["summary_human"] = "\n".join(doc["summary_human"])
    if "references" in doc:
        doc["references"] = [{"doi": ref["doi"], "id": ref.get("id", None)} for ref in doc["references"]]
    if "cited_by" in doc and len(doc["cited_by"]) > 0 and isinstance(doc["cited_by"][0], dict):
        doc["cited_by"] = [a["doi"] for a in doc["cited_by"]]
        
    if "cord_uid" in doc:
        del doc["cord_uid"]

    del doc['synced']

    if doc.get('hashed_title', None):
        del doc['hashed_title']
    vespa_doc = {
        'put': 'id:covid-19:doc::%s' % doc['id'],
        'fields': doc,
        'synced': doc.get('synced', False)
    }

    
    if abstract_embedding is None and not doc.get('abstract', None) in [None, [], ""]: 
        return None
    else:
        return vespa_doc


def process_chunk(chunk):
    client = pymongo.MongoClient(os.getenv("COVID_HOST"), username=os.getenv("COVID_USER"),
                                 password=os.getenv("COVID_PASS"), authSource=os.getenv("COVID_DB"))
    db = client[os.getenv("COVID_DB")]

    processed_docs = []
    for doc in chunk:
        processed_docs.append(doc_to_json(doc, db))

    db.entries_vespa_upload.insert_many(processed_docs)

    return processed_docs


if __name__ == '__main__':
    new_docs = db.entries_vespa2.find({'synced':False})
    processed_docs = []
    for doc in tqdm(list(new_docs), total = new_docs.count(),mininterval=20,maxinterval=60):
        processed_doc=doc_to_json(doc)
        if processed_doc is not None:
            db.entries_vespa_upload.replace_one({'put': processed_doc['put']}, processed_doc, upsert=True)
            #print(doc['id'])
            db.entries_vespa2.update_one({'_id': ObjectId(doc['id'])}, {"$set": {"synced": True}})
            #db.entries_vespa2.update_one({'id': ObjectId(doc['id'])}, {"$set": {"synced": True}})
            #processed_docs.append(processed_doc)
    #print(processed_docs)
    #db.entries_vespa_upload.insert_many(processed_docs)
    #db.entries_vespa2.update_many({"_id": {"$in": [e["fields"]['id'] for e in processed_docs]}},
                                 #{"$set": {"synced": True}})
        # processed_chunks = list(p.map(process_chunk, chunks))
