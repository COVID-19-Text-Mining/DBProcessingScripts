import os
import pymongo

client = pymongo.MongoClient(os.getenv("COVID_HOST"), username=os.getenv("COVID_USER"),
                             password=os.getenv("COVID_PASS"), authSource=os.getenv("COVID_DB"))
db = client[os.getenv("COVID_DB")]

parsed_collections = ['CORD_noncomm_use_subset',
  'CORD_comm_use_subset',
  'CORD_biorxiv_medrxiv',
  'CORD_custom_license',
  'CORD_metadata',
  "Scraper_connect_biorxiv_org",
  "google_form_submissions"]