import pickle
import os.path
import pickle
import os.path
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from getfilelistpy import getfilelist

SCOPES = 'https://www.googleapis.com/auth/drive.metadata.readonly'

creds = None

creFile = 'token.pickle'
if os.path.exists(creFile):
    with open(creFile, 'rb') as token:
        creds = pickle.load(token)
if not creds or not creds.valid:
    if creds and creds.expired and creds.refresh_token:
        creds.refresh(Request())
    else:
        flow = InstalledAppFlow.from_client_secrets_file(
            'client_secret.json', SCOPES)
        creds = flow.run_local_server()
    with open(creFile, 'wb') as token:
        pickle.dump(creds, token)

resource = {
    "oauth2": creds,
    "id": "1kKqZQh5v6YiW1lE8bS2q7ZrE0isHFYqu",
    "fields": "files(name,id)",
}
res = getfilelist.GetFileList(resource)  # or r = getfilelist.GetFolderTree(resource)
print(res)
