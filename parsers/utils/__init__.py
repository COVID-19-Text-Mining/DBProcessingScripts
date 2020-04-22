import requests


def clean_title(title):
    title = title.split("Running Title")[0]
    title = title.replace("^Running Title: ", "")
    title = title.replace("^Short Title: ", "")
    title = title.replace("^Title: ", "")
    title = title.strip()
    return title


def find_references(doi):
    """ Returns the references of a document as a <class 'list'> of <class 'dict'>.
    This is a list of documents cited by the current document.
    """
    if doi is None:
        return None

    references = []
    if doi:
        response = requests.get(f"https://opencitations.net/index/api/v1/references/{doi}").json()
        if response:
            references = [{"doi": r['cited'].replace("coci =>", "")} for r in response]

    if references:
        return references
    else:
        return None


def find_cited_by(doi):
    """ Returns the citations of a document as a <class 'list'> of <class 'str'>.
    A list of DOIs of documents that cite this document.
    """
    if doi is None:
        return None

    citations = []
    if doi:
        response = requests.get(f"https://opencitations.net/index/api/v1/citations/{doi}").json()
        if response:
            citations = [{"doi": r['citing'].replace("coci =>", ""), "text": r['citing'].replace("coci =>", "")} for r in response]
    if citations:
        return citations
    else:
        return None
