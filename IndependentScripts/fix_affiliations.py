from pymongo import MongoClient
import os
from tqdm import tqdm

client = MongoClient(
	host=os.environ["COVID_HOST"], 
	username=os.environ["COVID_USER"], 
	password=os.environ["COVID_PASS"],
	authSource=os.environ["COVID_DB"])

db = client[os.environ["COVID_DB"]]

form_submissions = db.entries.find({'authors.affiliation': {"$exists": True}})

for entry in tqdm(list(form_submissions)):
	fixed_authors = []
	for author in entry["authors"]:
		if "affiliation" in author and isinstance(author["affiliation"], str):
			author["affiliation"] = [author["affiliation"]]
		elif "affiliation" in author and isinstance(author["affiliation"], list):
			affs = []
			for aff in author["affiliation"]:
				if isinstance(aff, dict):
					affs.append(aff["name"])
				elif isinstance(aff, str):
					affs.append(aff)
				else:
					raise AttributeError()
			author["affiliation"] = affs
		elif "affiliation" in author and isinstance(author["affiliation"], dict):
			author["affiliation"] = [author["affiliation"]["name"]]
		fixed_authors.append(author)

	db.entries.update_one({"_id": entry["_id"], 'authors.affiliation': {"$exists":True}}, {"$set":{"authors":fixed_authors}})