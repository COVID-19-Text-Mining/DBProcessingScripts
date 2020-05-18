from abc import ABC, abstractmethod
from pprint import pprint

import regex
import requests
from mongoengine import (
    connect, Document, EmbeddedDocumentField,
    StringField, ListField,
    EmbeddedDocument, EmailField, ValidationError, DateTimeField, DynamicEmbeddedDocument, BooleanField, IntField)
from utils import find_remaining_ids
import pandas as pd
import numbers
import numpy as np
import xml.etree.ElementTree as ET

def correct_pd_dict(input_dict):
    """
        Correct the encoding of python dictionaries so they can be encoded to mongodb
        https://stackoverflow.com/questions/30098263/inserting-a-document-with-
        pymongo-invaliddocument-cannot-encode-object
        inputs
        -------
        input_dict : dictionary instance to add as document
        output
        -------
        output_dict : new dictionary with (hopefully) corrected encodings
    """

    output_dict = {}
    for key1, val1 in input_dict.items():
        # Nested dictionaries
        if isinstance(val1, dict):
            val1 = correct_pd_dict(val1)

        if isinstance(val1, np.bool_):
            val1 = bool(val1)

        if isinstance(val1, np.int64):
            val1 = int(val1)

        if isinstance(val1, np.float64):
            val1 = float(val1)

        if isinstance(val1, set):
            val1 = list(val1)

        output_dict[key1] = val1

    return output_dict

def drop_dirty_columns(input_pd,
                       to_drop=[],
                       drop_unnamed=True,
                       drop_num_title=True):
    to_drop = list(to_drop)

    for c in input_pd.columns:
        if drop_unnamed and str(c).startswith('Unnamed:'):
            to_drop.append(c)
        if drop_num_title and isinstance(c, numbers.Number):
            to_drop.append(c)
    print('columns to drop: ', to_drop)
    clean_pd = input_pd.drop(columns=to_drop)
    return clean_pd


class Parser(ABC):
    keys = [
        "doi",
        "title",
        "authors",
        "journal",
        "journal_short",
        "publication_date",
        "abstract",
        "origin",
        "source_display",
        "last_updated",
        "body_text",
        "has_full_text",
        "references",
        "cited_by",
        "link",
        "category_human",
        "keywords",
        "summary_human",
        "has_year",
        "has_month",
        "has_day",
        "is_preprint",
        "is_covid19",
        "license",
        "cord_uid",
        "pmcid",
        "pubmed_id",
        "who_covidence",
        "version",
        "copyright"
    ]

    def parse(self, doc):
        """
        Parses the input document into the standardized COVIDScholar entry format.
        Do not overwrite this method with your own 'parse' method!

        Args:
            doc: Whatever your input object is.

        Returns:

            (dict) Parsed entry.

        """
        doc = self._preprocess(doc)

        return self._postprocess(doc,
                                 {
                                     "doi": self._parse_doi(doc),
                                     "title": self._parse_title(doc),
                                     "authors": self._parse_authors(doc),
                                     "journal": self._parse_journal(doc),
                                     "journal_short": self._parse_journal_short(doc),
                                     "issn": self._parse_issn(doc),
                                     "publication_date": self._parse_publication_date(doc),
                                     "abstract": self._parse_abstract(doc),
                                     "origin": self._parse_origin(doc),
                                     "source_display": self._parse_source_display(doc),
                                     "last_updated": self._parse_last_updated(doc),
                                     "body_text": self._parse_body_text(doc),
                                     "has_full_text": self._parse_has_full_text(doc),
                                     "references": self._parse_references(doc),
                                     "cited_by": self._parse_cited_by(doc),
                                     "link": self._parse_link(doc),
                                     "category_human": self._parse_category_human(doc),
                                     "keywords": self._parse_keywords(doc),
                                     "summary_human": self._parse_summary_human(doc),
                                     "has_year": self._parse_has_year(doc),
                                     "has_month": self._parse_has_month(doc),
                                     "has_day": self._parse_has_day(doc),
                                     "is_preprint": self._parse_is_preprint(doc),
                                     "is_covid19": self._parse_is_covid19(doc),
                                     "license": self._parse_license(doc),
                                     "pmcid": self._parse_pmcid(doc),
                                     "pubmed_id": self._parse_pubmed_id(doc),
                                     "who_covidence": self._parse_who_covidence(doc),
                                     "version": self._parse_version(doc),
                                     "copyright": self._parse_copyright(doc),
                                     "cord_uid": self._parse_cord_uid(doc),
                                     "document_type": self._parse_document_type(doc)
                                 }
                                 )

class CDCNParser(Parser):
    ...

def insert_drugs_summary(mongo_db, excel_path):
    col_name = 'CDCN_drugs_summary'
    col = mongo_db[col_name]
    col.create_index('Treatment name', unique=True)

    # load data
    data = pd.read_excel(
        excel_path,
        sheet_name='Summary of drugs_pub',
        header=2
    )

    # clean data
    data = drop_dirty_columns(
        data,
        to_drop=['Example', ],
        drop_unnamed=True,
        drop_num_title=True,
    )
    data = data.replace({np.nan: None})

    # insert data
    for i in range(len(data)):
        entry = correct_pd_dict(data.iloc[i].to_dict())
        col.find_one_and_update(
            {
                'Treatment name': entry['Treatment name']
            },
            {
                "$set": entry,
            },
            upsert=True
        )

def insert_extracted_studies_summary(mongo_db, excel_path):
    col_name = 'CDCN_studies_summary'
    col = mongo_db[col_name]
    col.create_index('Citation', unique=True)

    # load data
    data = pd.read_excel(
        excel_path,
        sheet_name='Summary of extracted studies_pu',
        header=0
    )

    # TODO: convert Citation to doi

    # clean data
    sep_index = data[data['Citation'] == 'All papers above included in publication - CONTINUE PHASE 2 BELOW'].index[0]
    data = data.iloc[0:sep_index]
    data = data.replace({np.nan: None})

    # insert data
    for i in range(len(data)):
        entry = correct_pd_dict(data.iloc[i].to_dict())
        col.find_one_and_update(
            {
                'Citation': entry['Citation']
            },
            {
                "$set": entry,
            },
            upsert=True
        )

def insert_extracted_PubMed(mongo_db, excel_path):
    col_name = 'CDCN_extracted_PubMed'
    col = mongo_db[col_name]
    col.create_index('PMID', unique=True)

    # load data
    data = pd.read_excel(
        excel_path,
        sheet_name='PubMed Extracted_pub',
        header=4,
        dtype={
            'PMID': str,
        },
    )

    # clean data
    sep_index = data[data['CSTL Team'] == 'Number of Articles Extracted'].index[0]
    data = data.iloc[0:sep_index]
    data = data.replace({np.nan: None})

    data = data.replace({np.nan: None})

    # merge multi-row drug names
    drugs_in_papers = {}
    PMID = None
    for i in range(len(data)):
        entry = correct_pd_dict(data.iloc[i].to_dict())
        if entry['PMID'] is not None:
            PMID = entry['PMID']
        if not PMID:
            continue
        if PMID not in drugs_in_papers:
            drugs_in_papers[PMID] = []
        if (entry['Repurposed Drug Name'] is not None
                and entry['Repurposed Drug Name'] not in drugs_in_papers[PMID]):
            drugs_in_papers[PMID].append(entry['Repurposed Drug Name'])

    data['Repurposed Drug Name'] = data.apply(
        lambda x: drugs_in_papers.get(x['PMID'], [x['Repurposed Drug Name']]),
        axis=1
    )

    data = data[~data['PMID'].isna()]

    # insert data
    for i in range(len(data)):
        entry = correct_pd_dict(data.iloc[i].to_dict())
        col.find_one_and_update(
            {
                'PMID': entry['PMID']
            },
            {
                "$set": entry,
            },
            upsert=True
        )

def insert_extracted_BioRxivMedRxivChinaxiv(mongo_db, excel_path):
    col_name = 'CDCN_extracted_BioRxivMedRxivChinaxiv'
    col = mongo_db[col_name]
    col.create_index('DOI', unique=True)

    # TODO: collapse multiple lines
    # TODO: unify DOI as doi

    # load data
    data = pd.read_excel(
        excel_path,
        sheet_name='BioRxivMedRxivChinaxiv Extracte',
        header=0,
        dtype={
            'DOI': str,
        },
    )

    # clean data
    sep_index = data[data['CSTL Team'] == 'Number of Articles Extracted'].index[0]
    data = data.iloc[0:sep_index]
    data = data.replace({np.nan: None})

    # merge multi-row drug names
    drugs_in_papers = {}
    doi = None
    for i in range(len(data)):
        entry = correct_pd_dict(data.iloc[i].to_dict())
        if entry['DOI'] is not None:
            doi = entry['DOI']
        if not doi:
            continue
        if doi not in drugs_in_papers:
            drugs_in_papers[doi] = []
        if (entry['Repurposed Drug Name'] is not None
                and entry['Repurposed Drug Name'] not in drugs_in_papers[doi]):
            drugs_in_papers[doi].append(entry['Repurposed Drug Name'])

    data['Repurposed Drug Name'] = data.apply(
        lambda x: drugs_in_papers.get(x['DOI'], [x['Repurposed Drug Name']]),
        axis=1
    )

    data = data[~data['DOI'].isna()]

    # insert data
    for i in range(len(data)):
        entry = correct_pd_dict(data.iloc[i].to_dict())
        col.find_one_and_update(
            {
                'DOI': entry['DOI']
            },
            {
                "$set": entry,
            },
            upsert=True
        )

###########################################################
# map drugs to entries
###########################################################

def doi_url_rm_prefix(doi_url):
    doi = doi_url.strip()
    tmp_m = regex.match(r'.*doi.org/(.*)', doi_url)
    if tmp_m:
        doi = tmp_m.group(1).strip()
    return doi

def find_remaining_ids(id):
    """ Returns dictionary containing remaining relevant ids corresponding to
    the input id. Just input doi, pmid, or pmcid; function will return all three.
    Example output:
        {
            doi : 'doi_string',
            pmcid 'pmcid_string',
            pubmed_id : 'pubmed_id_string'
        }
    Returns None for either id if not available. Returns None for both ids if
    input id is None or request fails.
    """
    None_dict = {
        'doi': None,
        'pmcid': None,
        'pubmed_id': None
    }
    if id is None:
        return None_dict
    session = requests.Session()
    try:
        ids_url = 'https://www.ncbi.nlm.nih.gov/pmc/utils/idconv/v1.0/?ids=%s' % id
        response = session.get(ids_url)
        root = ET.fromstring(response.content)
        ids = dict()
    except:
        return None_dict
    record = root.find('record')
    if record is None:
        return None_dict
    if 'doi' in record.attrib:
        ids['doi'] = record.attrib['doi']
    else:
        ids['doi'] = None
    if 'pmcid' in record.attrib:
        ids['pmcid'] = record.attrib['pmcid']
    else:
        ids['pmcid'] = None
    if 'pmid' in record.attrib:
        ids['pubmed_id'] = record.attrib['pmid']
    else:
        ids['pubmed_id'] = None
    return ids

def update_PubMed_entries(mongo_db):
    col_name = 'CDCN_extracted_PubMed'
    col = mongo_db[col_name]
    col_entries = mongo_db['entries']
    col_entries_vespa = mongo_db['entries_vespa']
    query = col.find({}, )

    print('query.count()', query.count())
    found_entries = set()
    found_entries_vespa = set()
    for doc in query:
        PMID = doc['PMID']
        ids = find_remaining_ids(PMID)
        doi = ids['doi']
        drugs = doc.get('Repurposed Drug Name', None)
        if not drugs:
            continue
        # TODO: remove drug name NR
        if isinstance(drugs, str):
            drugs = [drugs]

        if doi is not None:
            print('doi', doi)
            col.find_one_and_update(
                {
                    '_id': doc['_id']
                },
                {
                    '$set': {
                        'doi': doi
                    }
                }
            )

        # # update entries
        # if doi is not None:
        #     entry = col_entries.find_one({'doi': doi})
        #     if entry:
        #         col_entries.find_one_and_update(
        #             {
        #                 '_id': entry['_id']
        #             },
        #             {
        #                 "$set": {
        #                     'drug_names': drugs
        #                 },
        #             }
        #         )
        #         found_entries.add(entry['_id'])
        #
        # # update vespa entries
        # if PMID is not None:
        #     entry = col_entries_vespa.find_one({'pubmed_id': PMID})
        #     if entry:
        #         col_entries_vespa.find_one_and_update(
        #             {
        #                 '_id': entry['_id']
        #             },
        #             {
        #                 "$set": {
        #                     'drug_names': drugs
        #                 },
        #             }
        #         )
        #         found_entries_vespa.add(entry['_id'])
        #
        # if doi is not None:
        #     entry = col_entries_vespa.find_one({'doi': doi})
        #     if entry:
        #         col_entries_vespa.find_one_and_update(
        #             {
        #                 '_id': entry['_id']
        #             },
        #             {
        #                 "$set": {
        #                     'drug_names': drugs
        #                 },
        #             }
        #         )
        #         found_entries_vespa.add(entry['_id'])

    print('found_entries', len(found_entries))
    print('found_entries_vespa', len(found_entries_vespa))

def update_BioRxivMedRxivChinaxiv_entries(mongo_db):
    # papers in BioRxivMedRxivChinaxiv do not have pmid naturally
    col_name = 'CDCN_extracted_BioRxivMedRxivChinaxiv'
    col = mongo_db[col_name]
    col_entries = mongo_db['entries']
    col_entries_vespa = mongo_db['entries_vespa']
    query = col.find({}, )

    print('query.count()', query.count())
    found_entries = set()
    found_entries_vespa = set()
    for doc in query:
        doi = doi_url_rm_prefix(doc['DOI'])
        drugs = doc['Repurposed Drug Name']
        if isinstance(drugs, str):
            drugs = [drugs]

        # if doi is not None:
        #     entry = col_entries.find_one({'doi': doi})
        #     if entry:
        #         col_entries.find_one_and_update(
        #             {
        #                 '_id': entry['_id']
        #             },
        #             {
        #                 "$set": {
        #                     'drug_names': drugs
        #                 },
        #             }
        #         )
        #         found_entries.add(entry['_id'])
        #
        # if doi is not None:
        #     entry = col_entries_vespa.find_one({'doi': doi})
        #     if entry:
        #         col_entries_vespa.find_one_and_update(
        #             {
        #                 '_id': entry['_id']
        #             },
        #             {
        #                 "$set": {
        #                     'drug_names': drugs
        #                 },
        #             }
        #         )
        #         found_entries_vespa.add(entry['_id'])

    print('found_entries', len(found_entries))
    print('found_entries_vespa', len(found_entries_vespa))

def update_studies_summary_entries(mongo_db):
    from IndependentScripts.common_utils import query_crossref
    from IndependentScripts.common_utils import text_similarity_by_char
    from IndependentScripts.common_utils import LEAST_TITLE_LEN
    from IndependentScripts.common_utils import FIVE_PERCENT_TITLE_LEN
    from IndependentScripts.common_utils import LEAST_TITLE_SIMILARITY
    from IndependentScripts.common_utils import IGNORE_BEGIN_END_TITLE_SIMILARITY

    def clean_title(title):
        clean_title = title
        clean_title = clean_title.lower()
        clean_title = clean_title.strip()
        return clean_title

    col_name = 'CDCN_studies_summary'
    col = mongo_db[col_name]
    found_entries_cr = set()
    query = col.find({
        'doi': {'$exists': False}
    })


    print('query.count()', query.count())
    for doc in query:
        query_params = {
            'sort': 'relevance',
            'order': 'desc',
            'query.bibliographic': doc['Full citations'],
        }
        try:
            crossref_results = query_crossref(query_params)
        except Exception as e:
            crossref_results = None
            print(e)

        if crossref_results is None:
            continue

        # filter out query results without DOI
        crossref_results = list(filter(
            lambda x: (
                'DOI' in x
                and isinstance(x['DOI'], str)
                and len(x['DOI']) > 0
            ),
            crossref_results
        ))

        # filter out query results without title or abstract
        crossref_results = list(filter(
            lambda x: (
                'title' in x
                and isinstance(x['title'], list)
                and len(x['title']) > 0
            ),
            crossref_results
        ))

        # match by title directly
        matched_item = None
        matched_candidates = []

        for item in crossref_results:
            if not ('title' in item
                and isinstance(item['title'], list)
                and len(item['title']) > 0
            ):
                continue
            if len(item['title']) != 1:
                print("len(item['title']) != 1", len(item['title']))
            cr_title = clean_title(item['title'][0])
            doc_title = clean_title(doc['Full citations'])
            doc_title = doc_title.split('.')
            doc_title = sorted(doc_title, key=len, reverse=True)
            doc_title = doc_title[0]
            similarity = text_similarity_by_char(
                cr_title,
                doc_title,
                enable_ignore_begin_end=True,
                ignore_begin_end_text_len=FIVE_PERCENT_TITLE_LEN,
                ignore_begin_end_similarity=0.6,
            )
            
            if (len(cr_title) > LEAST_TITLE_LEN
                and len(doc['Full citations']) > LEAST_TITLE_LEN
                and similarity > 0.85):
                matched_item = item
                col.find_one_and_update(
                    {
                        '_id': doc['_id']
                    },
                    {
                        '$set': {
                            'doi': item['DOI']
                        }
                    }
                )
                found_entries_cr.add(item['DOI'])
                break

    print('len(found_entries_cr)', len(found_entries_cr))

if __name__ == '__main__':
    from IndependentScripts.common_utils import get_mongo_db

    mongo_db = get_mongo_db('../config.json')

    # insert_drugs_summary(mongo_db=mongo_db, excel_path='../rsc/CDCN_CORONA.xlsx')
    # insert_extracted_studies_summary(mongo_db=mongo_db, excel_path='../rsc/CDCN_CORONA.xlsx')
    # insert_extracted_PubMed(mongo_db=mongo_db, excel_path='../rsc/CDCN_CORONA.xlsx')
    # insert_extracted_BioRxivMedRxivChinaxiv(mongo_db=mongo_db, excel_path='../rsc/CDCN_CORONA.xlsx')

    # update_PubMed_entries(mongo_db)
    # update_BioRxivMedRxivChinaxiv_entries(mongo_db)
    update_studies_summary_entries(mongo_db)