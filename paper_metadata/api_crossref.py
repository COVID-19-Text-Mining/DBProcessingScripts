import requests
import warnings

###########################################
# communicate with crossref
###########################################

def query_crossref_by_doi(doi, verbose=True):
    """
    get crossref records by paper doi

    :param doi: (str) doi of a paper
    :param verbose: (bool) print diagnosis message or not
    :return: (dict) result from crossref api
    """
    # goal
    crossref_results = None

    # query crossref
    query_url = 'https://api.crossref.org/works/{}'.format(doi)
    try:
        query_results = requests.get(
            query_url,
        )
    except:
        raise ConnectionError(
            'Request to crossref failed when searching doi: {}!'.format(doi)
        )
    try:
        query_results = query_results.json()
    #print(query_results)
    except Exception as e:
        raise ValueError(
            'Query result from crossref cannot be jsonified when searching doi: {}!'.format(doi)
        )

    # filter out empty query results
    if ('message' in query_results
        and isinstance(query_results['message'], dict)
    ):
        crossref_results = query_results['message']
    else:
        warnings.warn(
            'Empty result from crossref when searching doi: {}'.format(doi)
        )

    return crossref_results


def query_crossref(query_params):
    """
    Query crossref by arbitrary parameters
    Introduction to available parameters is on https://github.com/CrossRef/rest-api-doc
    The most useful one is to query by bibliographic (title/authors)

    :param query_params: (dict)
        example query by title
            query_params = {
                'sort': 'relevance',
                'order': 'desc',
                'query.bibliographic': title,
            }
        example query by authors
            query_params = {
                'sort': 'relevance',
                'order': 'desc',
                'query.bibliographic': ', '.join([x['last'] for x in author_names]),
            }
    :return: (dict) result from crossref api
    """
    # goal
    crossref_results = None

    # query crossref
    query_url = 'https://api.crossref.org/works'
    try:
        query_results = requests.get(
            query_url,
            params=query_params,
        )
    except:
        raise ConnectionError(
            'Request to crossref failed when querying by: {}!'.format(query_params)
        )
    try:
        query_results = query_results.json()
    except Exception as e:
        raise ValueError(
            'Query result from crossref cannot be jsonified when querying by: {}!'.format(query_params)
        )

    # filter out empty query results
    if ('message' in query_results
        and 'items' in query_results['message']
        and isinstance(query_results['message']['items'], list)
        and len(query_results['message']['items']) > 0
    ):
        crossref_results = query_results['message']['items']
    else:
        warnings.warn(
            'Empty result from crossref when querying by: {}!'.format(query_params)
        )

    return crossref_results
