#!/usr/bin/env python3
# Copyright Lawrence Berkeley National Laboratory. #

import os
import pymongo
import json
import re
from tqdm import tqdm
from utils import clean_title
import datetime

client = pymongo.MongoClient(os.getenv("COVID_HOST"), username=os.getenv("COVID_USER"),
                             password=os.getenv("COVID_PASS"), authSource=os.getenv("COVID_DB"))

db = client[os.getenv("COVID_DB")]

meta_keys = [
    'prism:url',
    'dc:identifier',
    'eid',
    'prism:doi',
    'pii',
    'dc:title',
    'prism:publicationName',
    'prism:aggregationType',
    'prism:issn',
    'prism:volume',
    'prism:issueIdentifier',
    'prism:startingPage',
    'prism:endingPage',
    'prism:pageRange',
    'prism:number',
    'dc:format',
    'prism:coverDate',
    'prism:coverDisplayDate',
    'prism:copyright',
    'prism:publisher',
    'dc:creator',
    'dc:description',
    'openaccess',
    'openaccessArticle',
    'openaccessType',
    'openArchiveArticle',
    'openaccessSponsorName',
    'openaccessSponsorType',
    'openaccessUserLicense',
    'link'
]


def parse_authors(authors):
    authors_parsed = []
    for i in authors:
        name = i['$']
        if ',' in name:
            name = ' '.join(map(lambda x: x.strip(), reversed(name.split(','))))
        authors_parsed.append({'name': name})
    return authors_parsed


def parse_link(link, doi):
    for i in link:
        if i["@rel"] == "scidir":
            url = i['@href']
            break
    if url is None:
        url = 'https://doi.org/' + doi
    return url


def fix_abstract(abstract):
    abstract = abstract or ''
    abstract = re.sub(r'\s+', ' ', abstract).strip()
    abstract = re.sub(r'^abstract\s+', '', abstract, flags=re.IGNORECASE)
    return abstract


def flatten_elsevier_entry(metadata, meta_keys=meta_keys):
    flattened_doc = {}
    for key in meta_keys:
        if key == "dc:creator":
            value = parse_authors(metadata["coredata"].get(key, []))
        elif key == "link":
            value = parse_link(metadata["coredata"].get(key, []), metadata["coredata"].get("prism:doi", []))
        elif key == "dc:description":
            value = fix_abstract(metadata["coredata"].get(key, ''))
        elif key == "dc:title":
            value = clean_title(metadata["coredata"].get(key, ''))
        else:
            value = metadata["coredata"].get(key, None)
        flattened_doc[key] = value

    flattened_doc["pubmed_id"] = metadata.get("pubmed-id", None)
    flattened_doc["scopus_id"] = metadata.get("scopus-id", None)
    flattened_doc["scopus_eid"] = metadata.get("scopus-eid", None)
    original_text = metadata.get("originalText", None)
    flattened_doc["original_text_unparsed"] = str(original_text) if original_text else None
    return flattened_doc


def format_for_vespa(flat_elsevier_entry):
    idx = flat_elsevier_entry["_id"]
    title = flat_elsevier_entry.get('dc:title', None)
    abstract = flat_elsevier_entry.get('dc:description', None)
    sha = None
    source = "Elsevier Novel Coronavirus Information Center"
    full_text_dir = None
    license = flat_elsevier_entry.get('openaccessUserLicense', None)
    journal = flat_elsevier_entry.get('prism:publicationName', None)
    url = flat_elsevier_entry.get('link', None)
    cord_uid = None
    pmcid = flat_elsevier_entry.get('pmcid', None)
    pubmed_id = flat_elsevier_entry.get('pubmed_id', None)
    if pubmed_id is not None:
        try:
            pubmed_id = int(pubmed_id)
        except:
            pass
    who_covidence = flat_elsevier_entry.get('WHO #Covidence', None)
    publish_time = flat_elsevier_entry.get('prism:coverDate', None)
    timestamp = 0
    try:
        timestamp = int(datetime.datetime.strptime(publish_time, '%Y-%m-%d').timestamp())
    except:
        pass
    doi = flat_elsevier_entry.get('prism:doi', None)
    has_full_text = None
    # if has_full_text:
    #     authors, abstract_paragraphs, body_paragraphs, bib_entries, abstract, body = parse_file(full_text_dir, sha)
    # else:
    #     authors, abstract_paragraphs, body_paragraphs, bib_entries, body = ([], {}, {}, [], None)

    authors = flat_elsevier_entry.get('dc:creator', None)

    bib_entries = None
    body = None

    conclusion = None
    results = None
    discussion = None
    methods = None
    background = None
    introduction = None

    if doi:
        doi = 'https://doi.org/%s' % doi

    vespa_doc = {
        'title': title,
        '_id': idx,
        'source': source,
        'license': license,
        'datestring': publish_time,
        'doi': doi,
        'url': url,
        'cord_uid': cord_uid,
        'authors': authors,
        'bib_entries': bib_entries,
        'abstract': abstract,
        'journal': journal,
        'body_text': body,
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
        'dataset_version': datetime.datetime.now().timestamp()
    }
    return vespa_doc


def parse_elsevier_corona_and_update_collections():
    metadata_docs = list(db.Elsevier_corona_meta.find({}))
    flattened_docs = []
    vespa_docs = []
    for metadata_doc in tqdm(metadata_docs):
        mds = metadata_doc["meta"]
        mds = mds.replace('\n', '\\n')
        mds = mds.replace(chr(468), "u")
        metadata = json.loads(mds)["full-text-retrieval-response"]
        flattened_doc = flatten_elsevier_entry(metadata)
        flattened_doc["elsevier_paper_id"] = metadata_doc["paper_id"]
        flattened_doc['_id'] = metadata_doc['_id']
        flattened_doc['mtime'] = metadata_doc['mtime']
        flattened_doc['atime'] = metadata_doc['atime']
        flattened_doc['version'] = metadata_doc.get('version', 1)
        flattened_docs.append(flattened_doc)
        vespa_docs.append(format_for_vespa(flattened_doc))

    db.drop_collection("Elsevier_corona_flattened")
    db.Elsevier_corona_flattened.insert_many(flattened_docs)
    db.drop_collection("Vespa_Elsevier_corona_parsed")
    db.Vespa_Elsevier_corona_parsed.insert_many(vespa_docs)
