import pymongo
import os
import datetime
from pprint import pprint
from collections  import defaultdict

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
def parse_biorxiv_doc(doc):
	parsed_doc = dict()
	parsed_doc['Title'] = doc['Title']
	parsed_doc['Doi'] = doc['Doi']
	parsed_doc['Origin'] = "Scraper_connect_biorxiv_org"
	parsed_doc['last_updated'] = datetime.datetime.now()
	parsed_doc['Link'] = doc['Link']

	parsed_doc['Journal'] = doc['Journal']

	parsed_doc['Publication Date'] = doc['Publication Date']

	author_list = doc["Authors"]
	for a in author_list:
		a['Name'] = a['fn'] + " " + a['ln']

	parsed_doc['Authors'] = author_list

	parsed_doc['Abstract'] = doc['Abstract']


	return parsed_doc
