import tarfile
import os.path
from typing import Optional, Dict, Tuple
from pathlib import Path
from googleapiclient.discovery import build
from google.oauth2.credentials import Credentials
from googleapiclient.http import MediaFileUpload
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
import pickle
import logging
import sys
import subprocess, shlex


def make_tar_file(output_dir: Path, output_file_name: str, source_dir: Path):
    if not output_file_name.endswith(".tar.gz"):
        output_file_name = output_file_name + ".tar.gz"
    if output_dir.exists() is False:
        output_dir.mkdir(parents=True, exist_ok=True)
    output_tar_file = output_dir / output_file_name

    if output_tar_file.exists() is False:
        with tarfile.open(output_tar_file.as_posix(), "w:gz") as tar:
            tar.add(source_dir.as_posix(), arcname=os.path.basename(source_dir.as_posix()))

def run_command( command ):
    subprocess.call(shlex.split(command))

class GDrive:
    def __init__(self, token_file_location: Path = Path("./files/token.pickle"),
                 secret_location: Path = Path("./files/client_secrets.json"),
                 garden_id: str = "1kKqZQh5v6YiW1lE8bS2q7ZrE0isHFYqu", gdrive_scope=None):
        if gdrive_scope is None:
            gdrive_scope = ['https://www.googleapis.com/auth/drive']
        self.gdrive_scope = gdrive_scope
        self.token_file_location: Path = token_file_location
        self.secret_location: Path = secret_location
        self.garden_id = garden_id
        self.cred: Optional[Credentials] = self.authorize()
        self.gdrive_service: Optional[build] = build('drive', 'v3', credentials=self.cred)
        self.logger = logging.getLogger("GDrive")
        self.logger.setLevel(logging.DEBUG)

    def authorize(self,
                  ) -> Credentials:
        creds = None
        # The file token.pickle stores the user's access and refresh tokens, and is
        # created automatically when the authorization flow completes for the first
        # time.
        if self.token_file_location.exists():
            creds = pickle.load(self.token_file_location.open('rb'))
        # If there are no (valid) credentials available, let the user log in.
        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                creds.refresh(Request())
            else:
                flow = InstalledAppFlow.from_client_secrets_file(
                    self.secret_location.as_posix(), self.gdrive_scope)
                creds = flow.run_local_server(port=0)
            # Save the credentials for the next run
            token = self.token_file_location.open('wb')
            pickle.dump(creds, token)
            token.close()
        return creds

    def gdrive_check_folder_exist(self, folder_name_filter: str,
                                  parent_id: str = "1kKqZQh5v6YiW1lE8bS2q7ZrE0isHFYqu") -> Tuple[bool, Optional[str]]:
        """
        Check if the file exist on GDrive or not, if exist, return true and the file id, if not return False, and None
        :param folder_name_filter: folder id
        :param parent_id: parent id
        :return:
            if exist, return true and the file id, if not return False, and None
        """
        files = self.gdrive_list_files(folder_name_filter, parent_id)
        for k, v in files.items():
            if v == folder_name_filter:
                return True, k
        return False, None

    def get_or_create_folder(self, folder_name: str, parent_id: str = "1kKqZQh5v6YiW1lE8bS2q7ZrE0isHFYqu") -> str:
        """
        creates folder and return folder id
        :param parent_id: parent folder id
        :param folder_name: desired folder name
        :return:
            folder id
        """
        status, folder_id = self.gdrive_check_folder_exist(folder_name_filter=folder_name, parent_id=parent_id)
        if status is True:
            return folder_id
        else:
            file_metadata = {
                'name': folder_name,
                'mimeType': 'application/vnd.google-apps.folder',
                'parents': [parent_id]
            }
            file = self.gdrive_service.files().create(body=file_metadata,
                                                      fields='id').execute()
            return file.get('id', None)

    def gdrive_list_files(self,
                          folder_name_filter: str,
                          parent_id: str = "1kKqZQh5v6YiW1lE8bS2q7ZrE0isHFYqu") -> Dict[str, str]:
        result: Dict[str, str] = dict()  # id -> path
        block_query = (
            "'{}' in parents and trashed=false".format(parent_id)
            if folder_name_filter is None
            else "'{}' in parents and trashed=false and name contains '{}' ".format(parent_id, folder_name_filter)
        )
        page_token = None
        while True:
            response, page_token = self.send_gdrive_file_list(q=block_query,
                                                              page_token=page_token)
            for file in response.get('files', []):
                file_id, file_name = file.get('id'), file.get('name')
                # print(f"[{file_id}] -> [{file_name}]")
                result[file_id] = file_name
            page_token = response.get('nextPageToken', None)
            if page_token is None:
                break
        return result

    def send_gdrive_file_list(self, q: str, page_token, spaces='drive', fields='nextPageToken, '
                                                                               'files(id, name, modifiedTime, size)'):
        response = self.gdrive_service.files().list(q=q,
                                                    spaces=spaces,
                                                    fields=fields,
                                                    pageToken=page_token).execute()
        return response, page_token

    def upload_file_to_folder(self, folder_id: str, file_path:Path):
        media = MediaFileUpload(file_path, mimetype="application/gzip", resumable=True)
        body = {"name": file_path.name, "parents": [folder_id]}
        request = self.gdrive_service.files().create(media_body=media, body=body)
        response = None
        status = False
        file_id = None
        while response is None:
            status, response = request.next_chunk()
            if status is None:
                status = True
                file_id = response["id"]
                self.logger.debug(f"Uploaded [{file_path.name}] with id [{file_id}] to folder_id [{folder_id}].")
            else:
                status = False
        return status, file_id
