from maggma.builders import MapBuilder
from maggma.stores import MongoStore
from parse_cord_papers import parse_cord_doc
from parse_biorxiv_papers import parse_biorxiv_doc
import os

client = pymongo.MongoClient(os.getenv("COVID_HOST"), username=os.getenv("COVID_USER"),
                             password=os.getenv("COVID_PASS"), authSource=os.getenv("COVID_DB"))
db = client[os.getenv("COVID_DB")]

collection_names = ['CORD_noncomm_use_subset',
  'CORD_comm_use_subset',
  'CORD_biorxiv_medrxiv',
  'CORD_custom_license',
  'CORD_metadata']

biorxiv_db = MongoStore(database=os.getenv("COVID_DB"),
                         collection_name="Scraper_connect_biorxiv_org",
                         host=os.getenv("COVID_HOST"),
                         username=os.getenv("COVID_USER"),
                         password=os.getenv("COVID_PASS"),
                         key="Doi",
                         lu_field="last_updated",
                         lu_type="datetime")

biorxiv_parsed_db = MongoStore(database=os.getenv("COVID_DB"),
                         collection_name="Scraper_connect_biorxiv_org_parsed",
                         host=os.getenv("COVID_HOST"),
                         username=os.getenv("COVID_USER"),
                         password=os.getenv("COVID_PASS"),
                         key="doi",
                         lu_field="last_updated",
                         lu_type="datetime")

biorxiv_builder = MapBuilder(source=biorxiv_db,
                           	 target=biorxiv_parsed_db,
                          	 ufn=lambda x: parse_biorxiv_doc(x, biorxiv_db.database),
                          	 incremental=True,
                           	 delete_orphans=False,
                           	 query=None,
                             store_process_time=False)

print("Scraper_connect_biorxiv_org")
biorxiv_builder.run()


CORD_builders = dict()
for collection in CORD_collection_names:
	db = MongoStore(database=os.getenv("COVID_DB"),
                             collection_name=collection,
                             host=os.getenv("COVID_HOST"),
                             username=os.getenv("COVID_USER"),
                             password=os.getenv("COVID_PASS"),
                             key="doi",
                             lu_field="last_updated",
                             lu_type="datetime")

  parsed_db = MongoStore(database=os.getenv("COVID_DB"),
                             collection_name=collection+"_parsed",
                             host=os.getenv("COVID_HOST"),
                             username=os.getenv("COVID_USER"),
                             password=os.getenv("COVID_PASS"),
                             key="doi",
                             lu_field="last_updated",
                             lu_type="datetime")

	CORD_builders[collection] = MapBuilder(source=db,
                               	 target=parsed_db,
                              	 ufn=lambda x: parse_cord_doc(x, collection),
                              	 incremental=True,
                               	 delete_orphans=False,
                               	 query={"doi": {"$exists": True}},
                                 store_process_time=False)

for collection, builder in CORD_builders.items():
	print(collection)
	builder.run()
