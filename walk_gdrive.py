import os
from typing import List, Optional, Dict, Tuple
from pathlib import Path
import pickle
from googleapiclient.discovery import build
from google.auth.transport.requests import Request
from google_auth_oauthlib.flow import InstalledAppFlow
from google.oauth2.credentials import Credentials


FOLDER = 'application/vnd.google-apps.folder'

def authorize(scopes: List[str],
              default_token_file_location: Path = Path("token.pickle"),
              default_secret_location: Path = Path("client_secrets.json")) -> Credentials:
    creds = None
    # The file token.pickle stores the user's access and refresh tokens, and is
    # created automatically when the authorization flow completes for the first
    # time.
    if default_token_file_location.exists():
        creds = pickle.load(default_token_file_location.open('rb'))
    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                default_secret_location.as_posix(), scopes)
            creds = flow.run_local_server(port=0)
        # Save the credentials for the next run
        token = default_token_file_location.open('wb')
        pickle.dump(creds, token)
        token.close()
    return creds

creds = authorize(scopes=['https://www.googleapis.com/auth/drive.metadata.readonly'])
service = build('drive', version='v3', credentials=creds)

def iterfiles(name=None, is_folder=None, parent=None, order_by='folder,name,createdTime'):
    q = []
    if name is not None:
        q.append("name = '%s'" % name.replace("'", "\\'"))
    if is_folder is not None:
        q.append("mimeType %s '%s'" % ('=' if is_folder else '!=', FOLDER))
    if parent is not None:
        q.append("'%s' in parents" % parent.replace("'", "\\'"))
    params = {'pageToken': None, 'orderBy': order_by}
    if q:
        params['q'] = ' and '.join(q)
    while True:
        response = service.files().list(**params).execute()
        for f in response['files']:
            yield f
        try:
            params['pageToken'] = response['nextPageToken']
        except KeyError:
            return

def walk(top='root', by_name=False):
    if by_name:
        top, = iterfiles(name=top, is_folder=True)
    else:
        top = service.files().get(fileId=top).execute()
        if top['mimeType'] != FOLDER:
            raise ValueError('not a folder: %r' % top)
    stack = [((top['name'],), top)]
    while stack:
        path, top = stack.pop()
        dirs, files = is_file = [], []
        for f in iterfiles(parent=top['id']):
            is_file[f['mimeType'] != FOLDER].append(f)
        yield path, top, dirs, files
        if dirs:
            stack.extend((path + (d['name'],), d) for d in reversed(dirs))

for kwargs in [{'top': 'root', 'by_name': True}, {}]:
    for path, root, dirs, files in walk(**kwargs):
        print('%s\t%d %d' % ('/'.join(path), len(dirs), len(files)))