import pymongo
import os
import datetime
from pprint import pprint
from collections  import defaultdict
import datetime

def clean_title(title):
    clean_title = title.split("Running Title")[0]
    clean_title = clean_title.replace("^Running Title: ", "")
    clean_title = clean_title.replace("^Short Title: ", "")
    clean_title = clean_title.replace("^Title: ", "")
    clean_title = clean_title.strip()
    return clean_title

#Entries collection format
"""
{
*"Title": string,
"Authors": {
[*"Name": string (preference for first initial last format),
"Affiliation": string,
"Email": string
]
}
*"Doi": string,
"Journal" string,
"Publication Date": Datetime.Datetime object,
"Abstract": string,
"Origin": string, source scraped from/added from,
"Last Updated: Datetime.Datetime object, timestamp of last update,
"Body Text": {
["Section Heading": string,
"Text": string
]
}
"Citations": list of dois,
"Link": use given if available, otherwise build from doi
"Category_human": string,
"Category_ML": string,
"Tags_human": list of strings,
"Tags_ML": list of strings,
"Relevance_human": string,
"Relevance_ML": string,
"Summary_human": string,
"Summary_ML": string

}
"""
       
def parse_cord_doc(doc, collection_name):
    parsed_doc = dict()
    parsed_doc['title'] = doc['metadata']['title']
    parsed_doc['doi'] = doc['doi'].strip()
    parsed_doc['origin'] = collection_name
    parsed_doc['link'] = "https://doi.org/%s"%parsed_doc['doi']

    try:
        parsed_doc["journal"] = doc['journal_name']
    except KeyError:
        parsed_doc["journal"] = None

    if 'crossref_raw_result' in doc.keys():
        if 'publish_date' in doc.keys():
            pd = doc['publish_date']
        elif 'crossref_raw_result' in doc.keys() and 'published-print' in doc['crossref_raw_result'].keys():
            pd = doc['crossref_raw_result']['published-print']['date-parts'][0]
        else:
            pd = None

        publication_date_dict = defaultdict(lambda: 1)
        if isinstance(pd, dict):
            for k,v in pd.items():
                if v is not None:
                    publication_date_dict[k] = v

            if 'year' in pd.keys() and pd['year'] is not None:
                #Year is mandatory
                parsed_doc['publication_date'] = datetime.datetime(year=pd['year'], month=publication_date_dict['month'], day=publication_date_dict['day'])
                parsed_doc['has_year'] = True

                parsed_doc['has_month'] = 'month' in publication_date_dict.keys()
                parsed_doc['has_day'] = 'day' in publication_date_dict.keys()

            else:
                parsed_doc['publication_date'] = None

        elif isinstance(pd, list):
            if len(pd) == 2 and all([x is not None for x in pd]):
                parsed_doc['publication_date'] = datetime.datetime(year=pd[0], month=pd[1], day=1)      

                parsed_doc['has_year'] = True
                parsed_doc['has_month'] = True
                parsed_doc['has_day'] = False

            elif len(pd) >= 1 and pd[0] is not None:
                parsed_doc['publication_date'] = datetime.datetime(year=pd[0], month=1, day=1)      
                parsed_doc['has_year'] = True
                parsed_doc['has_month'] = False
                parsed_doc['has_day'] = False


        else:
            parsed_doc["publication_date"] = None
            parsed_doc['has_year'] = False
            parsed_doc['has_month'] = False
            parsed_doc['has_day'] = False

        # except KeyError:
        #   pprint(doc)
        #   exit(1)
        #   parsed_doc['publication_date'] = None

        author_list = []
        for a in doc['metadata']['authors']:
            author = dict()
            name = ""
            if a['first'] and a['first'] != "":
                name += a['first']
            if len(a['middle']) > 0:
                name += " "+" ".join([m for m in a['middle']])
            if a['last'] and a['last'] != "":
                name += " "+a['last']
            if a['suffix'] and a['suffix'] != "":
                name += " "+a['suffix']
            author['name'] = name

            if a['email'] != "":
                author['email'] = a['email'].strip()

            if 'institution' in a['affiliation'].keys():
                author['affiliation'] = a['affiliation']['institution']

            if len(author['name']) > 3:
                author_list.append(author)

        parsed_doc['authors'] = author_list

        if 'abstract' in doc['crossref_raw_result'].keys() and len(doc['crossref_raw_result']['abstract']) > 0:
            parsed_doc['abstract'] = doc['crossref_raw_result']['abstract']
        else:
            abstract = ""
            for t in doc['abstract']:
                abstract += t['text']

            parsed_doc['abstract'] = abstract

    else:
        parsed_doc['abstract'] = doc['csv_raw_result']['abstract']
        parsed_doc['authors'] = [{'name':a} for a in doc['csv_raw_result']['authors'].split(';')]
        parsed_doc['journal'] = doc['csv_raw_result']['journal']
        try:
            parsed_doc['publication_date'] = datetime.datetime.strptime(doc['csv_raw_result']['publish_time'],'%Y-%m-%d')
        except ValueError:
            try:
                parsed_doc['publication_date'] = datetime.datetime.strptime(doc['csv_raw_result']['publish_time'],'%Y %b %d')
            except ValueError:
                try:
                    parsed_doc['publication_date'] = datetime.datetime.strptime(doc['csv_raw_result']['publish_time'],'%Y %b')
                except ValueError:
                    parsed_doc['publication_date'] = datetime.datetime.strptime(doc['csv_raw_result']['publish_time'],'%Y')
                

    if 'abstract' in parsed_doc.keys() and parsed_doc['abstract'] is not None:
        if 'a b s t r a c t' in parsed_doc['abstract']:
            parsed_doc['abstract'] = parsed_doc['abstract'].split('a b s t r a c t')[1]


    sections = dict()
    for t in doc['body_text']:
        try:
            sections[t['section']] = sections[t['section']] + t['text']
        except KeyError:
            sections[t['section']] = t['text']

    sections_list = [{"Section Heading": k, "Text": v} for k,v in sections.items()]
    parsed_doc['body_text'] = sections_list

    citations_list = []
    for entry in doc['bib_entries'].values():
        if "DOI" in entry['other_ids'].keys():
            citations_list.append(entry['other_ids']['DOI'])


    parsed_doc['citations'] = citations_list
    return parsed_doc
