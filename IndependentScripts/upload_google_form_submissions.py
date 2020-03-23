from __future__ import print_function
import pickle
import os.path
from googleapiclient.discovery import build
from googleapiclient import http, errors
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from pprint import pprint
import datetime
import pymongo
import os
import gridfs

SCOPES = ['https://www.googleapis.com/auth/spreadsheets.readonly',
    'https://www.googleapis.com/auth/drive.readonly']

SAMPLE_SPREADSHEET_ID = "1mjnPab5eo5wCu0UQLXt_HM8yXR6pWfLr5zfqzp2GLFM"
#Current number of fields we have
SAMPLE_RANGE_NAME = 'A2:J'

collection_name = "google_form_submissions"
client = pymongo.MongoClient(os.getenv("COVID_HOST"), username=os.getenv("COVID_USER"),
                             password=os.getenv("COVID_PASS"), authSource=os.getenv("COVID_DB"))
db = client[os.getenv("COVID_DB")]

present_rows = set([e['row_id'] for e in db[collection_name].find()])
#Entries collection format
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
paper_fs = gridfs.GridFS(db, collection=collection_name + '_fs')

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

def parse_row(i, row):
    doc = dict()
    doc['last_updated'] = datetime.datetime.strptime(row[0],"%m/%d/%Y %H:%M:%S")
    doc['submission_email'] = row[1]
    doc['pdf_location'] = row[2]
    try:
        doc['file_id'] = doc['pdf_location'].split('open?id=')[1]
    except IndexError:
        doc['file_id'] = ""
    doc['link'] = row[3]
    doc['doi'] = row[4]
    doc['category_human'] = [row[5]]
    doc['keywords'] = row[6].split(',')
    doc['summary_human'] = row[7]
    doc['abstract'] = row[8]
    doc['relevance_human'] = row[9]
    doc['row_id'] = i

    return doc


def main():
    """Shows basic usage of the Sheets API.
    Prints values from a sample spreadsheet.
    """
    creds = None
    # The file token.pickle stores the user's access and refresh tokens, and is
    # created automatically when the authorization flow completes for the first
    # time.
    if os.path.exists('token.pickle'):
        with open('token.pickle', 'rb') as token:
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

    service = build('sheets', 'v4', credentials=creds)

    # Call the Sheets API
    sheet = service.spreadsheets()
    result = sheet.values().get(spreadsheetId=SAMPLE_SPREADSHEET_ID,
                                range=SAMPLE_RANGE_NAME).execute()

    values = result.get('values', [])

    if not values:
        print('No data found.')
    else:
        for i, row in enumerate(values):
            # Print columns A and E, which correspond to indices 0 and 4.
            doc = parse_row(i, row)
            if not doc['row_id'] in present_rows:
                pprint(doc)
                # service = build('drive', 'v3', credentials=creds)
                # local_file_dir = '/tmp/{}'.format(doc['file_id'])
                # download_file(service, doc['file_id'], open(local_file_dir,'wb'))
                # doc['PDF_gridfs_id'] = paper_fs.put(
                #         open(local_file_dir,'rb'),
                #         filename=doc['file_id'],
                #         manager_collection=collection_name,
                #     )

                db[collection_name].insert_one(doc)

if __name__ == '__main__':
    main()