import datetime
import re
import traceback
from collections import Counter
from io import StringIO, BytesIO
import pathlib
import os
import pickle
from googleapiclient.discovery import build
from googleapiclient import http, errors
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from pprint import pprint

import gridfs
from pdfminer.converter import TextConverter
from pdfminer.layout import LAParams, LTContainer, LTTextBox, LTLayoutContainer, LTTextLineHorizontal, \
    LTTextBoxHorizontal, LTTextBoxVertical
from pdfminer.pdfdocument import PDFDocument
from pdfminer.pdfinterp import PDFResourceManager, PDFPageInterpreter
from pdfminer.pdfpage import PDFPage
from pdfminer.pdfparser import PDFParser
from pdfminer.utils import Plane, uniq


def group_textlines(self, laparams, lines):
    """Patched class method that fixes empty line aggregation, and allows
    run-time line margin detection"""
    plane = Plane(self.bbox)
    plane.extend(lines)
    boxes = {}
    for line in lines:
        neighbors = line.find_neighbors(plane, laparams.line_margin)
        if line not in neighbors or not line.get_text().strip():
            continue

        # Correct margin to paragraph specific
        true_margin = laparams.line_margin
        for obj1 in neighbors:
            if obj1 is line:
                continue
            margin = min(abs(obj1.y0 - line.y1), abs(obj1.y1 - line.y0))
            margin = margin * 1.05 / line.height
            if margin < true_margin:
                true_margin = margin

        neighbors = line.find_neighbors(plane, true_margin)
        if line not in neighbors:
            continue

        members = []
        for obj1 in neighbors:
            if not obj1.get_text().strip():
                continue
            members.append(obj1)
            if obj1 in boxes:
                members.extend(boxes.pop(obj1))
        if isinstance(line, LTTextLineHorizontal):
            box = LTTextBoxHorizontal()
        else:
            box = LTTextBoxVertical()
        for obj in uniq(members):
            box.add(obj)
            boxes[obj] = box
    done = set()
    for line in lines:
        if line not in boxes:
            continue
        box = boxes[line]
        if box in done:
            continue
        done.add(box)
        if not box.is_empty():
            yield box
    return


# Patch the method
LTLayoutContainer.group_textlines = group_textlines


class TextHandler(TextConverter):
    def __init__(self, rsrcmgr):
        super(TextHandler, self).__init__(
            rsrcmgr, StringIO(), laparams=LAParams(line_margin=2.5))

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

    def is_ending_char(c):
        return re.match(r'!\.\?', c) is not None

    paragraphs = []

    for i, page in enumerate(device.get_true_paragraphs()):
        for j, p in enumerate(sorted(page, key=paragraph_pos_rank)):
            text = p['text'].strip()

            if j == 0 and len(paragraphs) > 0:
                if (not is_ending_char(paragraphs[-1][-1]) and
                        not text[0].isupper()):
                    paragraphs[-1] = paragraphs[-1] + ' ' + text
                    continue
            paragraphs.append(text)

    return paragraphs


def clean_title(title):
    title = title.split("Running Title")[0]
    title = title.replace("^Running Title: ", "")
    title = title.replace("^Short Title: ", "")
    title = title.replace("^Title: ", "")
    title = title.strip()
    return title

def download_file(service, file_id, local_fd):
  """Download a Drive file's content to the local filesystem.

  Args:
    service: Drive API Service instance.
    file_id: ID of the Drive file that will downloaded.
    local_fd: io.Base or file object, the stream that the Drive file's
        contents will be written to.
  """
  request = service.files().get_media(fileId=file_id)
  media_request = http.MediaIoBaseDownload(local_fd, request)

  while True:
    try:
      download_progress, done = media_request.next_chunk()
    except errors.HttpError as error:
      print('An error occurred: %s') % error
      return
    if download_progress:
      print('Download Progress: {}'.format(download_progress.progress()))
    if done:
      print('Download Complete')
      return

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


def parse_google_forms_doc(doc, db):
    parsed_doc = dict()
    parsed_doc['title'] = doc['title']
    parsed_doc['doi'] = doc['doi']
    parsed_doc['origin'] = "google_form_submissions"
    parsed_doc['last_updated'] = datetime.datetime.now()

    parsed_doc['category_human'] = doc['category_human']
    parsed_doc['summary_human'] = doc['summary_human']
    parsed_doc['relevance_human'] = doc['relevance_human']

    for field in ['link', 'journal', 'publication_date', 'abstract']:
        try:
            parsed_doc[field] = doc[field]
        except KeyError:
            pass
    try:
        #Made an oopsie in the form submission uploader and forgot to specify that the keywords are from humans
        parsed_doc['keywords_human'] = doc['keywords']
    except KeyError:
        pass

    author_list = []
    if 'authors' in doc.keys():
        for a in doc['authors']:
            author = dict()
            if 'name' in a.keys() and len(a['name']) > 3:
                name = a['name']
            else:
                name = ""
                if a['first'] and a['first'] != "":
                    name += a['first']
                if len(a['middle']) > 0:
                    name += " "+" ".join([m for m in a['middle']])
                if a['last'] and a['last'] != "":
                    name += " "+a['last']
                if a['suffix'] and a['suffix'] != "":
                    name += " "+a['suffix']
            author['name'] = name

            if 'email' in a.keys() and a['email'] != "":
                author['email'] = a['email'].strip()

            if isinstance(a['affiliation'], dict) and 'institution' in a['affiliation'].keys():
                author['affiliation'] = a['affiliation']['institution']
            elif isinstance(a['affiliation'], list):
                author['affiliation'] = a['affiliation']

            if len(author['name']) > 3:
                author_list.append(author)

        parsed_doc['authors'] = author_list

    creds = None
    # The file token.pickle stores the user's access and refresh tokens, and is
    # created automatically when the authorization flow completes for the first
    # time.
    pathlib.Path(__file__).parent.absolute()
    token_path = os.path.join(pathlib.Path(__file__).parent.absolute(), 'token.pickle')
    if os.path.exists(token_path):
        with open(token_path, 'rb') as token:
            creds = pickle.load(token)
    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                'credentials.json', SCOPES)
            creds = flow.run_local_server(port=0)
        # Save the credentials for the next run
        with open('token.pickle', 'wb') as token:
            pickle.dump(creds, token)

    service = build('drive', 'v3', credentials=creds)
    local_file_dir = '/tmp/{}'.format(doc['file_id'])
    download_file(service, doc['file_id'], open(local_file_dir,'wb'))

    paper_fs = gridfs.GridFS(
        db, collection='google_form_submissions_fs')

    doc['PDF_gridfs_id'] = paper_fs.put(
            open(local_file_dir,'rb'),
            filename=doc['file_id'],
            manager_collection='google_form_submissions',
        )

    pdf_file = paper_fs.get(doc['PDF_gridfs_id'])

    try:
        paragraphs = extract_paragraphs_pdf(BytesIO(pdf_file.read()))
    except Exception as e:
        print('Failed to extract PDF %s(%r) (%r)' % (doc['Doi'], doc['PDF_gridfs_id'], e))
        traceback.print_exc()
        paragraphs = []

    parsed_doc['body_text'] = [{
        'section_heading': None,
        'text': x
    } for x in paragraphs]



    return parsed_doc
