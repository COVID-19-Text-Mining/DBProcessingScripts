class CrossrefParser(object):
    # TODO: need to extend base parser and use the schema for auto type check
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.parse_functions = {
            'journal_name': self._parse_journal_name,
            'ISSN': self._parse_ISSN,
            'publish_date': self._parse_publish_data,
            'crossref_reference': self._parse_reference,
            'title': self._parse_title,
            'doi': self._parse_doi,
            'authors': self._parse_authors,
            'abstract': self._parse_abstract,
        }

    def _parse_journal_name(self, data):
        # journal_name
        journal_name = None

        if (journal_name is None
            and 'container-title' in data
            and isinstance(data['container-title'], list)
            and len(data['container-title']) == 1
        ):
            journal_name = data['container-title'][0]

        if (journal_name is None
            and 'short-container-title' in data
            and isinstance(data['short-container-title'], list)
            and len(data['short-container-title']) == 1
        ):
            journal_name = data['short-container-title'][0]

        return journal_name

    def _parse_ISSN(self, data):
        ISSN = None
        # ISSN
        if (ISSN is None
            and 'ISSN' in data
            and isinstance(data['ISSN'], list)
            and len(data['ISSN']) == 1
        ):
            ISSN = data['ISSN'][0]
        return ISSN

    def _parse_date(self, date_obj):
        date = {}
        if isinstance(date_obj, list):
            date = self._parse_date_list(date_obj)
        if len(date) < 3:
            for k in ({'year', 'month', 'day'} - set(date.keys())):
                date[k] = None
        return date

    def _parse_date_list(self, date_list):
        time_parsed = {}
        if (len(date_list) == 3
            and int(date_list[0]) > 999
            and int(date_list[0]) < 10000
        ):
            time_parsed = {
                'year': int(date_list[0]),
                'month': int(date_list[1]),
                'day': int(date_list[2]),
            }
        if (len(date_list) == 2
            and int(date_list[0]) > 999
            and int(date_list[0]) < 10000
        ):
            time_parsed = {
                'year': int(date_list[0]),
                'month': int(date_list[1]),
            }
        return time_parsed

    def _parse_publish_data(self, data):
        # publish_date
        publish_date = None
        if (publish_date is None
            and 'issued' in data
            and 'date-parts' in data['issued']
            and isinstance(data['issued']['date-parts'], list)
            and len(data['issued']['date-parts']) == 1
            and len(data['issued']['date-parts'][0]) > 0
        ):
            publish_date = data['issued']['date-parts'][0]
        if (publish_date is None
            and 'published-online' in data
            and 'date-parts' in data['published-online']
            and isinstance(data['published-online']['date-parts'], list)
            and len(data['published-online']['date-parts']) == 1
            and len(data['published-online']['date-parts'][0]) > 0
        ):
            publish_date = data['published-online']['date-parts'][0]
        if (publish_date is None
            and 'published-print' in data
            and 'date-parts' in data['published-print']
            and isinstance(data['published-print']['date-parts'], list)
            and len(data['published-print']['date-parts']) == 1
            and len(data['published-print']['date-parts'][0]) > 0
        ):
            publish_date = data['published-print']['date-parts'][0]
        if publish_date is not None:
            publish_date = self._parse_date(publish_date)
        return publish_date

    def _parse_title(self, data):
        # title
        title = None

        if (title is None
            and 'title' in data
            and isinstance(data['title'], list)
            and len(data['title']) > 0
            and len(data['title'][0]) > 0
        ):
            title = data['title'][0]

        return title

    def _parse_reference(self, data):
        # reference
        crossref_reference = None

        if (crossref_reference is None
            and 'reference' in data
            and isinstance(data['reference'], list)
            and len(data['reference']) > 0
        ):
            crossref_reference = []
            for ref in data['reference']:
                if ('DOI' in ref
                    and isinstance(ref['DOI'], str)
                    and len(ref['DOI']) > 0
                ):
                    crossref_reference.append(ref['DOI'])
            if len(crossref_reference) == 0:
                crossref_reference = None

        return crossref_reference

    def _parse_doi(self, data):
        # doi
        doi = None

        if (doi is None
            and 'DOI' in data
            and isinstance(data['DOI'], str)
            and len(data['DOI']) > 0
        ):
            doi = data['DOI']
        return doi

    def _parse_authors(self, data):
        # authors
        authors = None

        if (authors is None
            and 'author' in data
            and isinstance(data['author'], list)
            and len(data['author']) > 0
        ):
            authors = []
            for x in data['author']:
                if not ('given' in x and 'family' in x):
                    continue
                authors.append({
                    'first': x['given'],
                    'middle': [],
                    'last': x['family'],
                    'suffix': '',
                    'affiliation': {},
                    'email': '',
                })
            if len(authors) == 0:
                authors = None
        return authors

    def _parse_abstract(self, data):
        # abstract
        abstract = None

        if (abstract is None
            and 'abstract' in data
            and isinstance(data['abstract'], str)
            and len(data['abstract']) > 0
        ):
            abstract = data['abstract']
        return abstract

    def parse(self, data):
        paper = {}

        for key, parse_func in self.parse_functions.items():
            result = parse_func(data)
            if result:
                paper[key] = result

        if len(paper) == 0:
            paper = None
        return paper