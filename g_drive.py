import pickle
import os.path
from googleapiclient.discovery import build
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from typing import List, Optional, Dict, Tuple
from pathlib import Path
import google
from typing import Set
import pprint
from tqdm import tqdm


def authorize(scopes: List[str],
              default_token_file_location: Path = Path("./files/token.pickle"),
              default_secret_location: Path = Path("./files/client_secrets.json")) -> Credentials:
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


def send_gdrive_file_list(drive_service,
                          q: str,
                          page_token,
                          spaces='drive',
                          fields='nextPageToken, '
                                 'files(id, name, modifiedTime, size)'):
    response = drive_service.files().list(q=q,
                                          spaces=spaces,
                                          fields=fields,
                                          pageToken=page_token).execute()
    return response, page_token


def get_blocks(drive_service,
               garden_folder_id: str = "1kKqZQh5v6YiW1lE8bS2q7ZrE0isHFYqu",
               block_filter: Optional[str] = None) -> Dict[str, str]:
    result: Dict[str, str] = dict()  # id -> path
    block_query = (
        "'{}' in parents".format(garden_folder_id)
        if block_filter is None
        else "'{}' in parents and name contains '{}'".format(garden_folder_id, block_filter)
    )
    page_token = None
    while True:
        response, page_token = send_gdrive_file_list(drive_service=drive_service,
                                                     q=block_query,
                                                     page_token=page_token)
        for file in response.get('files', []):
            file_id, file_name = file.get('id'), file.get('name')
            print(f"[{file_id}] -> [{file_name}]")
            result[file_id] = file_name
        page_token = response.get('nextPageToken', None)
        if page_token is None:
            break
    return result


def get_launchers(drive_service, folder_id: str, folder_path: Path("./")) -> dict:
    launchers: Dict[str, str] = dict()  # path/name -> id

    def recurse(service, folder_id, base_folder_path=Path(".")):
        page_token = None
        query = "'{}' in parents".format(folder_id)

        # first find all folders with launchers in its name but .tar.gz not in its name
        while True:
            response, page_token = send_gdrive_file_list(drive_service=service, q=query, page_token=page_token)
            for entry in response["files"]:
                entry_id, entry_name = entry["id"], entry["name"]
                new_folder_path = Path(base_folder_path) / entry_name
                # print(f"{entry_id} -> {entry_name}")
                if "launcher" in entry_name and ".tar.gz" in entry_name:
                    # this is the end of recursion
                    launchers[new_folder_path.as_posix()] = entry_id
                elif "launcher" in entry_name:
                    # we need to recurse on this folder
                    launchers[new_folder_path] = entry_id
                    recurse(service=service, folder_id=entry_id, base_folder_path=new_folder_path.as_posix())
                else:
                    pass

            page_token = response.get("nextPageToken", None)
            if page_token is None:
                break  # done with launchers in current block

    recurse(drive_service, folder_id=folder_id, base_folder_path=folder_path)
    return launchers


def get_all_remote_launchers() -> dict:
    directory_path: dict = dict()
    SCOPES = ['https://www.googleapis.com/auth/drive.metadata.readonly']
    creds: Credentials = authorize(scopes=SCOPES)
    service = build('drive', 'v3', credentials=creds)

    blocks: dict = get_blocks(service, garden_folder_id="1kKqZQh5v6YiW1lE8bS2q7ZrE0isHFYqu")
    for block_id, block_path in tqdm(blocks.items()):
        curr_launchers = get_launchers(drive_service=service, folder_id=block_id, folder_path=block_path)
        directory_path[block_path] = curr_launchers
    return directory_path


def gdrive_list_files(folder_name_filter: str,
                      parent_id: str = "1kKqZQh5v6YiW1lE8bS2q7ZrE0isHFYqu") -> Dict[str, str]:
    result: Dict[str, str] = dict()  # id -> path
    block_query = (
        "'{}' in parents and trashed=false".format(parent_id)
        if folder_name_filter is None
        else "'{}' in parents and trashed=false and name contains '{}' ".format(parent_id, folder_name_filter)
    )
    page_token = None
    while True:
        response, page_token = send_gdrive_file_list(drive_service=drive_service,
                                                     q=block_query,
                                                     page_token=page_token)
        for file in response.get('files', []):
            file_id, file_name = file.get('id'), file.get('name')
            # print(f"[{file_id}] -> [{file_name}]")
            result[file_id] = file_name
        page_token = response.get('nextPageToken', None)
        if page_token is None:
            break
    return result


def gdrive_check_folder_exist(folder_name_filter: str,
                              parent_id: str = "1kKqZQh5v6YiW1lE8bS2q7ZrE0isHFYqu") -> Tuple[bool, Optional[str]]:
    """
    Check if the file exist on GDrive or not, if exist, return true and the file id, if not return False, and None
    :param folder_name_filter: folder id
    :param parent_id: parent id
    :return:
        if exist, return true and the file id, if not return False, and None
    """
    files = gdrive_list_files(folder_name_filter, parent_id)
    for k, v in files.items():
        if v == folder_name_filter:
            return True, k
    return False, None


def gdrive_create_folder(folder_name: str, parent_id: str = "1kKqZQh5v6YiW1lE8bS2q7ZrE0isHFYqu") -> str:
    """
    creates folder and return folder id
    :param parent_id: parent folder id
    :param folder_name: desired folder name
    :return:
        folder id
    """
    status, folder_id = gdrive_check_folder_exist(folder_name_filter=folder_name, parent_id=parent_id)
    if status is True:
        return folder_id
    else:
        file_metadata = {
            'name': folder_name,
            'mimeType': 'application/vnd.google-apps.folder',
            'parents': [parent_id]
        }
        file = drive_service.files().create(body=file_metadata,
                                            fields='id').execute()
        return file.get('id', None)


if __name__ == "__main__":
    directory_path: dict = dict()
    SCOPES = ['https://www.googleapis.com/auth/drive']
    creds: Credentials = authorize(scopes=SCOPES)
    drive_service = build('drive', 'v3', credentials=creds)
    folder_id = gdrive_create_folder(folder_name="Invoice")
    print(folder_id)
