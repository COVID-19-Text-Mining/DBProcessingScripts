from maggma.builders import MapBuilder
from maggma.stores import MongoStore
from parse_elsevier_corona import parse_elsevier_doc
import os
import pymongo

client = pymongo.MongoClient(os.getenv("COVID_HOST"), username=os.getenv("COVID_USER"),
                             password=os.getenv("COVID_PASS"), authSource=os.getenv("COVID_DB"))
db = client[os.getenv("COVID_DB")]

elsevier_db = MongoStore(database=os.getenv("COVID_DB"),
                         collection_name="Elsevier_corona_xml",
                         host=os.getenv("COVID_HOST"),
                         username=os.getenv("COVID_USER"),
                         password=os.getenv("COVID_PASS"),
                         key="paper_id",
                         lu_field="last_updated",
                         lu_type="datetime")

elsevier_parsed_db = MongoStore(database=os.getenv("COVID_DB"),
                         collection_name="Elsevier_corona_xml_parsed",
                         host=os.getenv("COVID_HOST"),
                         username=os.getenv("COVID_USER"),
                         password=os.getenv("COVID_PASS"),
                         key="doi",
                         lu_field="last_updated",
                         lu_type="datetime")

elsevier_builder = MapBuilder(source=elsevier_db,
                             target=elsevier_parsed_db,
                             ufn=lambda x: parse_elsevier_doc(x, db),
                             incremental=True,
                             delete_orphans=False,
                             query=None,
                             store_process_time=False)

print("Elsevier")
elsevier_builder.run()

