import json
import os

# change the default path to scopus config file
os.environ['PYB_CONFIG_FILE'] = os.path.abspath('scopus_config.ini')

from pprint import pprint
import requests
import warnings
import pybliometrics
from pybliometrics.scopus import ScopusSearch
from pybliometrics.scopus import config as scopus_config

###########################################
# communicate with scopus
###########################################

def query_scopus_by_doi(doi, verbose=True):
    """
    get crossref records by paper doi

    :param doi: (str) doi of a paper
    :param verbose: (bool) print diagnosis message or not
    :return: (dict) result from crossref api
    """
    # goal
    scopus_results = None

    # query crossref
    query_results = ScopusSearch(
        'DOI({})'.format(doi),
        max_entries=None,
        cursor=True
    )

    # filter out empty query results
    if query_results.results is not None:
        scopus_results = query_results.results[0]._asdict()
    else:
        warnings.warn(
            'Empty result from scopus when searching doi: {}'.format(doi)
        )

    return scopus_results

def change_default_scopus_config(api_key, cache_dir=None):
    # scopus_config.set()
    if not cache_dir:
        cache_dir = os.path.abspath(os.path.join(__file__, '../scopus_cache'))
    if not os.path.exists(cache_dir):
        os.makedirs(cache_dir)
    scopus_config["Authentication"]["APIKey"] = api_key
    for k in scopus_config._sections['Directories']:
        tmp_path = scopus_config._sections['Directories'][k]
        scopus_config._sections['Directories'][k] = os.path.join(
            cache_dir,
            os.path.basename(tmp_path)
        )

if __name__ == '__main__':
    with open('../config.json', 'r') as fr:
       credentials = json.load(fr)

    change_default_scopus_config(
        api_key=credentials['scopus']['api_key']
    )
    r = query_scopus_by_doi('10.1016/j.biomaterials.2006.02.011')
    # r = query_scopus_by_doi('10.1016/j.biomaterials.2006.02.1')
    pprint(r)
