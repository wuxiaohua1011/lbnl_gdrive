from typing import List, Iterable, Union, Any, Dict, Tuple
from maggma.core.builder import Builder
from maggma.core.store import Store
from pathlib import Path
import os
from pprint import pprint
import re
from itertools import chain
from g_drive.models import GDriveLog, TaskRecord
from datetime import datetime
from typing import Optional
from googleapiclient.discovery import build
from google.oauth2.credentials import Credentials
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
from .utilities import make_tar_file
from .utilities import GDrive


class GDriveBuilder(Builder):
    def __init__(self, sources: Union[List[Store], Store],
                 targets: Union[List[Store], Store],
                 mp_ids_to_upload: List[str],
                 root_dir_path: Path,
                 token_file_location: Path = Path("./files/token.pickle"),
                 secret_location: Path = Path("./files/client_secrets.json"),
                 garden_id: str = "1kKqZQh5v6YiW1lE8bS2q7ZrE0isHFYqu",
                 output_dir: Path = Path("./output"),
                 gdrive_scope=None):
        super().__init__(sources, targets)
        if gdrive_scope is None:
            gdrive_scope = ['https://www.googleapis.com/auth/drive']
        self.root_dir_path = root_dir_path
        self.mongo_record_store = self.sources[0]
        self.tasks_mongo_store = self.sources[1]
        self.mp_ids_to_upload = mp_ids_to_upload
        self.gdrive = GDrive(token_file_location=token_file_location,
                             secret_location=secret_location,
                             garden_id=garden_id,
                             gdrive_scope=gdrive_scope)
        self.output_dir: Path = output_dir

        if root_dir_path.exists() is False:
            raise NotADirectoryError(f"Root Directory {root_dir_path} does not exist")
        if output_dir.exists() is False:
            self.logger.debug(f"creating tmp directory {output_dir}")
            output_dir.mkdir(parents=True, exist_ok=True)

    def get_items(self) -> Tuple[str, List[str]]:
        """
        Scan all files from root_dir

        see if they are on mongodb, return a set that are not on mongodb (aka not yet synced)

        :return:
            (block, [path, path, path])
        """
        # scan local file system for blocks and launchers path
        # {block -> [path, path, path, path]}
        # find all blessed tasks
        # fetch from missing launchers from GDrive from HPSS
        """
        1. Find a list of block launchers from blessed tasks
        """
        # get dir_paths for all passed in mp_ids
        paths_dict: Dict[str, List[str]] = self.get_path_dict()
        pprint.pprint(paths_dict)
        paths_dict: Dict[str, List[str]] = self.scan_local_file_system(block_pattern="^block", end_pattern="^launcher")

        for block_name, paths in paths_dict.items():
            yield block_name, paths

    def get_path_dict(self) -> Dict[str, List[str]]:
        result: Dict[str, List[str]] = dict()
        records = self.tasks_mongo_store.query(criteria={"task_id": {"$in": self.mp_ids_to_upload} }, properties=["task_id", "dir_name", "dir_name_full", "last_updated"])
        for record in records:
            task_record = TaskRecord.parse_obj(record)
            block_name = task_record.dir_name.split('/')[0]
            launcher_names = task_record.dir_name.split("/")[1:]
            if block_name not in result:
                result[block_name] = self.organize_launcher_names_from_launcher_names(launcher_names)
        return result

    def organize_launcher_names_from_launcher_names(self, launcher_names: List[str]):
        """
        turn [launcher-xxx, launcher-yyy, launcher-ccc] into
        [launcher-xxx, launcher-xxx/launcher-yyy, launcher-xxx/launcher-yyy/launcher-ccc]

        :param launcher_names: list of launcher names
        :return: list of launcher names

        """
        self.logger.error("organize_launcher_names_from_launcher_names IS NOT IMPLEMENTED YET")
        return launcher_names

    def process_item(self, item: Tuple[str, List[str]]) -> Any:
        """

        :param item:
        :return:
        """

        """
        For each block
            1. run tape restore (which will only restore files that are NOT in local file system)
            2. upload to GDrive
        """
        block_name, launcher_paths = item[0], item[1]
        self.debug_msg(f"Processing Block [{block_name}]")
        # create temporary directory that contains zipped up launchers
        self.compress_launchers(block_name, launcher_paths)
        logs: List[GDriveLog] = self.upload_to_gdrive(block_name=block_name, launcher_paths=launcher_paths)
        return logs

    def update_targets(self, items: List[List[GDriveLog]]):
        self.debug_msg(f"Updating local mongo database")
        # for item_list in items:
        #     self.debug_msg(f"Updating {item_list[0].block_name} with [{len(item_list) - 1}] launchers")
        flatten_list: List[GDriveLog] = list(chain.from_iterable(items))
        self.mongo_record_store.update(docs=[log.dict() for log in flatten_list], key="path")

    def finalize(self):
        super(GDriveBuilder, self).finalize()
        # self.remove_all_content_in_output_dir()

    def scan_local_file_system(self, block_pattern="^block", end_pattern="^launcher") -> Dict[str, List[str]]:
        # find blocks
        blocks: List[str] = []
        for block_name in os.listdir(self.root_dir_path.as_posix()):
            matches = re.findall(block_pattern, block_name)
            if len(matches) > 0:
                blocks.append(block_name)

        # find corresponding launchers
        launchers: Dict[str, List[str]] = dict()
        for block in blocks:
            self.scan_local_file_helper(block=block, folder_prefix_path=Path(block), pattern=end_pattern, log=launchers)
        return launchers

    def scan_local_file_helper(self, block, folder_prefix_path: Path, pattern: str, log: dict):
        for folder_name in os.listdir((self.root_dir_path / folder_prefix_path).as_posix()):
            patterns = re.findall(pattern=pattern, string=folder_name)
            if len(patterns) > 0:
                if block not in log:
                    log[block] = [(Path(folder_prefix_path) / folder_name).as_posix()]
                else:
                    log[block].append((Path(folder_prefix_path) / folder_name).as_posix())

                self.scan_local_file_helper(block=block, folder_prefix_path=folder_prefix_path / folder_name,
                                            pattern=pattern, log=log)

    def check_if_block_exist(self, block_name) -> bool:
        count = self.mongo_record_store.count(criteria={"block_name": block_name})
        return True if count > 0 else False

    def find_new_launcher_paths(self, block_name: str, launcher_paths: List[str]) -> List[str]:
        if self.check_if_block_exist(block_name=block_name) is False:
            return launcher_paths
        else:
            mongo_store_launcher_paths = self.find_mongo_store_launcher_paths(block_name)
            launcher_path_diff = list(set(launcher_paths) - set(mongo_store_launcher_paths))
            return launcher_path_diff

    def find_mongo_store_launcher_paths(self, block_name) -> List[str]:
        launcher_paths_raw: Dict[str] = self.mongo_record_store.query_one(criteria={"block_name": block_name},
                                                                          properties={"launcher_paths": 1})
        launcher_paths = launcher_paths_raw['launcher_paths']
        return launcher_paths

    def debug_msg(self, msg):
        self.logger.debug(msg)

    def upload_new_block(self, block_name, launcher_paths) -> List[GDriveLog]:
        """
        Upload an entire new block to google drive.
        :param block_name: the name of the block
        :param launcher_paths: the launchers that this block contains
        :return:
            list of GDrive records, to sync with mongo_record_store
        """
        self.debug_msg(f"Uploading new block {block_name} with {len(launcher_paths)} launchers")
        # TODO upload to gdrive
        # self.upload_to_gdrive(block_name, launcher_paths)

        block_gdrive_log = GDriveLog(
            GDriveID="test",
            path=block_name,
            last_updated=datetime.now(),
            created_at=datetime.now(),
            block_name=block_name,
            is_block=True,
            launcher_paths=launcher_paths
        )
        launcher_logs = [GDriveLog(
            GDriveID="test",
            path=(Path(block_name) / launcher_path).as_posix(),
            last_updated=datetime.now(),
            created_at=datetime.now(),
            block_name=block_name,
            is_block=False,
            launcher_name=launcher_path
        ) for launcher_path in launcher_paths]

        all_gdrive_logs = [block_gdrive_log] + launcher_logs
        return all_gdrive_logs

    def upload_launchers_to_block(self, block_name: str, launcher_paths: List[str]) -> List[GDriveLog]:
        self.debug_msg(f"Uploading to block {block_name} with {len(launcher_paths)} new launchers")
        # TODO upload to gdrive

        block_gdrive_log: GDriveLog = GDriveLog.parse_obj(self.mongo_record_store.query_one(criteria={"$and": [
            {"block_name": block_name},
            {"is_block": True}]
        }
        ))
        block_gdrive_log.launcher_paths.append(launcher_paths)
        launcher_logs = [GDriveLog(
            GDriveID="test",
            path=(Path(block_name) / launcher_path).as_posix(),
            last_updated=datetime.now(),
            created_at=datetime.now(),
            block_name=block_name,
            is_block=False,
            launcher_name=launcher_path
        ) for launcher_path in launcher_paths]
        logs_to_update = [block_gdrive_log] + launcher_logs
        return logs_to_update

    def upload_to_gdrive(self, block_name: str, launcher_paths: List[str]) -> List[GDriveLog]:
        self.logger.debug(f"Uploading {block_name} to Gdrive")
        # create folder if block folder does not already exist on g drive
        # otherwise, get the block folder id
        result: List[GDriveLog] = []
        if self.check_if_block_exist(block_name=block_name):
            folder_gdrive_log: GDriveLog = self.get_block_record_from_mongo(block_name=block_name)
            folder_id = folder_gdrive_log.GDriveID
        else:
            folder_id = self.gdrive.get_or_create_folder(folder_name=block_name)
            folder_gdrive_log: GDriveLog = GDriveLog(
                GDriveID=folder_id,
                path=block_name,
                block_name=block_name,
                is_block=True,
                launcher_paths=[]
            )
        result.append(folder_gdrive_log)
        # if launcher*.tar.gz is not in gdrive, upload
        # otherwise, don't do anything with that launcher
        for launcher_path in launcher_paths[0: 1]:
            launcher_status, launcher = self.is_launcher_in_gdrive(launcher_path)
            if launcher_status is True:
                pass
            else:
                status, launcher_gz_path = self.find_launcher_gz_path(Path(launcher_path))
                status, file_id = self.gdrive.upload_file_to_folder(folder_id=folder_id,
                                                                    file_path=Path(launcher_gz_path))
                gdrive_log = GDriveLog(
                    GDriveID=file_id,
                    path=launcher_path,
                    block_name=block_name,
                    is_block=False,
                    launcher_name=launcher_path
                )
                result.append(gdrive_log)
                folder_gdrive_log.launcher_paths.append(launcher_path)
        return result

    def compress_launchers(self, block_name: str, launcher_paths: List[str]):
        self.logger.debug(f"Compressing lauchers for {block_name}")
        for launcher_path in launcher_paths:
            make_tar_file(output_dir=self.output_dir / block_name,
                          output_file_name=launcher_path.split("/")[-1],
                          source_dir=self.root_dir_path / launcher_path)

    def is_launcher_in_gdrive(self, launcher_path) -> Tuple[bool, Optional[GDriveLog]]:
        item: Optional[dict] = self.mongo_record_store.query_one(criteria={"path": launcher_path})
        return (False, None) if item is None else (True, GDriveLog.parse_obj(item))

    def find_launcher_gz_path(self, launcher_path: Path) -> Tuple[bool, Optional[Path]]:
        launcher_gz_path = Path((self.output_dir / launcher_path).as_posix() + ".tar.gz")
        return (True, launcher_gz_path) if launcher_gz_path.exists() else (False, None)

    def get_block_record_from_mongo(self, block_name) -> Optional[GDriveLog]:
        record = self.mongo_record_store.query_one(criteria={"$and": [{"block_name": block_name}, {"is_block": True}]})
        return None if record is None else GDriveLog.parse_obj(record)

    def remove_all_content_in_output_dir(self):
        import os
        import shutil

        for root, dirs, files in os.walk(self.output_dir.as_posix()):
            for f in files:
                os.unlink(os.path.join(root, f))
            for d in dirs:
                shutil.rmtree(os.path.join(root, d))

