import requests
import datetime
import pymongo
import os
from pprint import pprint
from google_form_doi_check import valid_a_doi

crossref_session = requests.Session()
def crossref_data(doi):
    crossref_url = 'https://api.crossref.org/works/%s?mailto=amalietrewartha@lbl.gov' % doi
    try:
        response = crossref_session.get(crossref_url)
    except:
        print('request to cross_ref failed!')
        return
    try:
        response = response.json()
        cr_response = response['message']
        if 'items' in cr_response.keys():
            cr_response = cr_response['items'][0]
        #Curate metadata for db entry
        cr_metadata = dict()
        cr_metadata['crossref_raw_result'] = response
        if 'container-title' in cr_response.keys():
            cr_metadata['journal'] = cr_response['container-title'] #Get journal title
        elif 'short-container-title' in cr_response.keys():
            cr_metadata['journal'] = cr_response['short-container-title'] #Get short journal title as alternative 
        cr_metadata['title'] = cr_response['title'] #Get title
        date = cr_response['issued']['date-parts']
        try:
            date = "{0}/{1}/{2}".format(date[0][1], date[0][2], date[0][0])
            cr_metadata['publication_date'] = datetime.datetime.strptime(date, "%m/%d/%Y") #Get date
        except IndexError:
            cr_metadata['publication_date'] = datetime.datetime.strptime(str(date[0][0]), "%Y")
        if 'author' in cr_response:
            authors = []
            for author in cr_response['author']:
                if 'given' in author:
                    name = '{0}. {1}'.format(author['given'][0], author['family'])
                elif 'family' in author:
                    name = '{0}'.format(author['family'])
                else:
                    name = author['name']
                affiliation = author['affiliation']
                authors.append({'name' : name, 'affiliation' : affiliation})
            cr_metadata['authors'] = authors #Get authors
        if 'reference' in cr_response:
            cr_refdois = []
            references    = cr_response['reference']
            for citation in references:
                if 'DOI' in citation:
                    cr_refdois.append(citation['DOI'])
                    cr_metadata['citations'] = cr_refdois #Get dois of citations
    except Exception as e:
        print('query result cannot be jsonified!')
        print('response.text', response.text)
        print('response.status_code', response.status_code)
        print('response.reason', response.reason)
        print()
        return
    return cr_metadata

collection_name = "google_form_submissions"
client = pymongo.MongoClient(os.getenv("COVID_HOST"), username=os.getenv("COVID_USER"),
                             password=os.getenv("COVID_PASS"), authSource=os.getenv("COVID_DB"))
db = client[os.getenv("COVID_DB")]

for e in db[collection_name].find({"crossref_raw_result": {"$exists" : False}}):
    print("Searching for metadata for row {}, doi {}".format(e['row_id'], e['doi']))
    if valid_a_doi(e['doi'], e):
    #First validate the doi
        cr_metadata = crossref_data(e['doi'])
        if cr_metadata is not None:
            pprint("Found row metadata for row {}".format(e['row_id']))
            e = {**e, **cr_metadata}
            db[collection_name].find_one_and_replace({"_id": e['_id']}, e)
    else:
        print("doi invalid!")
