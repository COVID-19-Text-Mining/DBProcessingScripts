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
def parse_cord_doc(doc, collection_name):
	parsed_doc = dict()
	parsed_doc['title'] = doc['metadata']['title']
	parsed_doc['doi'] = doc['doi'].strip()
	parsed_doc['origin'] = collection_name
	parsed_doc['last_updated'] = datetime.datetime.now()
	parsed_doc['link'] = "https://doi.org/%s"%parsed_doc['doi']

	try:
		parsed_doc["journal"] = doc['journal_name']
	except KeyError:
		parsed_doc["journal"] = None

	try:
		publication_date_dict = defaultdict(lambda: 1)

		for k,v in doc['publish_date']:
			publication_date_dict[k] = v

		#Year is mandatory
		parsed_doc['publication_date'] = Datetime.Datetime(year=doc['publish_date']['year'], month=publication_date_dict['month'], day=publication_date_dict['day'])

		parsed_doc["publication_date"] = publication_date_dict
	except KeyError:
		parsed_doc['publication_date'] = None

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
		author['Name'] = name

		if a['email'] != "":
			author['Email'] = a['email'].strip()

		if 'institution' in a['affiliation'].keys():
			author['Affiliation'] = a['affiliation']['institution']

		if len(author['Name']) > 3:
			author_list.append(author)

	parsed_doc['authors'] = author_list

	abstract = ""
	for t in doc['abstract']:
		abstract += t['text']

	parsed_doc['abstract'] = abstract

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
