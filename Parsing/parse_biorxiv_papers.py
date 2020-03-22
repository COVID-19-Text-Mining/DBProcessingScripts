import datetime
import re
from collections import Counter
from io import StringIO

from pdfminer.converter import TextConverter
from pdfminer.layout import LAParams, LTContainer, LTTextBox
from pdfminer.pdfdocument import PDFDocument
from pdfminer.pdfinterp import PDFResourceManager, PDFPageInterpreter
from pdfminer.pdfpage import PDFPage
from pdfminer.pdfparser import PDFParser


class TextHandler(TextConverter):
    def __init__(self, rsrcmgr):
        super(TextHandler, self).__init__(
            rsrcmgr, StringIO(), laparams=LAParams())

        self.pages = []

    def receive_layout(self, ltpage):
        paragraphs = []

        def render(item):
            if isinstance(item, LTTextBox):
                paragraphs.append({
                    'text': item.get_text(),
                    'bbox': item.bbox
                })

            if isinstance(item, LTContainer):
                for child in item:
                    render(child)

        render(ltpage)
        self.pages.append(paragraphs)

    def get_true_paragraphs(self):
        # Drop redundant paragraphs
        counter_by_page = Counter()
        for page in self.pages:
            for item in page:
                counter_by_page[item['text']] += 1
        redundant = set(x for x, y in counter_by_page.items() if y > 1)
        for page in self.pages:
            page[:] = list(filter(
                lambda x: x['text'] not in redundant,
                page
            ))

        # Drop number only paragraphs
        for page in self.pages:
            page[:] = list(filter(
                lambda x: len(re.findall(r'[a-zA-Z]', x['text'])) > 0.5 * len(x['text']),
                page
            ))

        # Convert newlines and excessive whitespaces
        for page in self.pages:
            for word in page:
                word['text'] = re.sub(r'\s+', ' ', word['text'])

        return self.pages


def extract_paragraphs_pdf(pdf_file):
    """
    pdf_file is a file-like object.
    This function will return lists of plain-text paragraphs."""
    parser = PDFParser(pdf_file)
    doc = PDFDocument(parser)
    rsrcmgr = PDFResourceManager()
    device = TextHandler(rsrcmgr)
    interpreter = PDFPageInterpreter(rsrcmgr, device)
    for page in PDFPage.create_pages(doc):
        interpreter.process_page(page)

    def paragraph_pos_rank(p):
        x, y = p['bbox'][0], -p['bbox'][1]
        return int(y)

    paragraphs = []

    for i, page in enumerate(device.get_true_paragraphs()):
        for j, p in enumerate(sorted(page, key=paragraph_pos_rank)):
            if j == 0 and len(paragraphs) > 0:
                if paragraphs[-1][-1].islower() and p[0].islower():
                    paragraphs[-1] = paragraphs[-1] + ' ' + p
                    continue
            paragraphs.append(p['text'])

    return paragraphs


def clean_title(title):
    title = title.split("Running Title")[0]
    title = title.replace("^Running Title: ", "")
    title = title.replace("^Short Title: ", "")
    title = title.replace("^Title: ", "")
    title = title.strip()
    return title


# Entries collection format
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


def parse_biorxiv_doc(doc):
    parsed_doc = dict()
    parsed_doc['Title'] = doc['Title']
    parsed_doc['Doi'] = doc['Doi']
    parsed_doc['Origin'] = "Scraper_connect_biorxiv_org"
    parsed_doc['last_updated'] = datetime.datetime.now()
    parsed_doc['Link'] = doc['Link']

    parsed_doc['Journal'] = doc['Journal']

    parsed_doc['Publication Date'] = doc['Publication Date']

    author_list = doc["Authors"]
    for a in author_list:
        a['Name'] = a['fn'] + " " + a['ln']

    parsed_doc['Authors'] = author_list

    parsed_doc['Abstract'] = doc['Abstract']

    return parsed_doc
