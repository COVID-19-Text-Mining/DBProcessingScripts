#! /bin/bash                                                                                                                                                                                                

#source /global/homes/a/amaliet/.bash_profile                                                                                                                                                               
#cov_python=/global/homes/a/amaliet/.conda/envs/covidscholar/bin/python                                                                                                                                     
cd DBProcessingScripts/parsers
python run_all_parsers_vespa.py
cd ../ML_builders
python is_covid19_class.py
python Keywords.py
cd ../parsers
python mongo_to_feed_mongo.py