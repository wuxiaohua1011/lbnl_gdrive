from typing import List, Iterable, Union, Any, Dict, Tuple
from maggma.core.builder import Builder
from maggma.stores.mongolike import MongoStore
from maggma.core.store import Store
from pathlib import Path
import os
from pprint import pprint
import re
from itertools import chain
from g_drive.models import GDriveLog, TaskRecord
from datetime import datetime
from typing import Optional
# from googleapiclient.discovery import build
# from google.oauth2.credentials import Credentials
import pickle
import os.path
# from googleapiclient.discovery import build
# from google.oauth2.credentials import Credentials
# from google_auth_oauthlib.flow import InstalledAppFlow
# from google.auth.transport.requests import Request
from typing import List, Optional, Dict, Tuple
from pathlib import Path
import google
from typing import Set
import pprint
from tqdm import tqdm
from .utilities import make_tar_file, run_command
# from .utilities import GDrive
from maggma.core.store import Sort



class GDriveBuilder(Builder):
    def __init__(self, sources: Union[List[Store], Store],
                 targets: Union[List[Store], Store],
                 mp_ids_to_upload: List[str],
                 source_root_dir: Union[Path, str],
                 token_file_location: Path = Path("./files/token.pickle"),
                 secret_location: Path = Path("./files/client_secrets.json"),
                 garden_id: str = "1kKqZQh5v6YiW1lE8bS2q7ZrE0isHFYqu",
                 temporary_output_dir: Union[Path, str] = Path("./output"),
                 limit: int = 1000,
                 gdrive_scope=None):
        super().__init__(sources, targets)
        if gdrive_scope is None:
            gdrive_scope = ['https://www.googleapis.com/auth/drive']
        self.limit: int = limit
        self.source_root_dir: Path = Path(source_root_dir)
        self.mongo_record_store = self.sources[0]
        self.tasks_mongo_store = self.sources[1]
        self.materials_mongo_store = self.sources[2]
        self.mp_ids_to_upload = mp_ids_to_upload
        # self.gdrive = GDrive(token_file_location=token_file_location,
        #                      secret_location=secret_location,
        #                      garden_id=garden_id,
        #                      gdrive_scope=gdrive_scope)
        self.output_dir: Path = Path(temporary_output_dir)
        self.emmet_restore_file_path: Path = self.output_dir / f"emmet_restore_{datetime.now()}.txt"
        self.rclone_flags = ["--multi-thread-streams=5"]
        self.rclone_options = ["-P"]
        if self.source_root_dir.exists() is False:
            raise NotADirectoryError(f"Root Directory {source_root_dir} does not exist")
        if self.output_dir.exists() is False:
            self.logger.debug(f"creating tmp directory {temporary_output_dir}")
            temporary_output_dir.mkdir(parents=True, exist_ok=True)

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

        # paths_dict: Dict[str, List[str]] = self.scan_local_file_system(block_pattern="^block", end_pattern="^launcher")
        # pprint.pprint(paths_dict)
        """
        1. Find a list of block launchers from blessed tasks
        """
        # get dir_paths for all passed in mp_ids
        paths = self.get_paths()
        paths_organized: Dict[str, List[str]] = self.organize_path(paths=paths)
        for block_name, launcher_paths in paths_organized.items():
            yield block_name, launcher_paths

    def process_item(self, item: Tuple[str, List[str]]) -> Any:
        """
        For each block
                    1. zip up the block
        :param item:
        :return:
        """
        block_name, launcher_paths = item[0], item[1]
        self.debug_msg(f"Processing Block [{block_name}]")
        self.compress_launchers(block_name, launcher_paths)

    def update_targets(self, items: List[List[GDriveLog]]):
        self.debug_msg(f"Updating local mongo database")
        self.upload_to_gdrive()
        # so that we have a cache of what we've uploaded
        self.update_local_mongo_db()

    def upload_to_gdrive(self):
        cmd = "rclone"
        for flag in self.rclone_flags:
            cmd += " " + flag

        cmd += " copy "
        for option in self.rclone_options:
            cmd += " " + option
        cmd += f"{self.output_dir.as_posix()} remote: "
        run_command(cmd)

    def update_local_mongo_db(self):
        pass

    def finalize(self):
        super(GDriveBuilder, self).finalize()
        # self.remove_all_content_in_output_dir()

    def debug_msg(self, msg):
        self.logger.debug(msg)

    def compress_launchers(self, block_name: str, launcher_paths: List[str]):
        self.logger.debug(f"Compressing [{len(launcher_paths)}] launchers for [{block_name}]")
        for launcher_path in launcher_paths:
            source_dir = self.source_root_dir / launcher_path
            make_tar_file(output_dir=self.output_dir / block_name,
                          output_file_name=launcher_path.split("/")[-1],
                          source_dir=source_dir)

    def remove_all_content_in_output_dir(self):
        import os
        import shutil

        for root, dirs, files in os.walk(self.output_dir.as_posix()):
            for f in files:
                os.unlink(os.path.join(root, f))
            for d in dirs:
                shutil.rmtree(os.path.join(root, d))

    def get_paths(self) -> List[str]:
        materials = self.materials_mongo_store.query(criteria={"deprecated": False,
                                                               "task_id": {"$in": self.mp_ids_to_upload}},
                                                     properties={"task_id": 1, "blessed_tasks": 1,
                                                                 "last_updated": 1},
                                                     sort={"last_updated": Sort.Descending}, limit=self.limit)
        task_ids_to_query: List[str] = []
        for material in materials:
            if "blessed_tasks" in material:
                blessed_tasks: dict = material["blessed_tasks"]
                task_ids_to_query.extend(list(blessed_tasks.values()))
            else:
                print(f"material [{material['task_id']}] does not have blessed tasks")

        tasks = self.tasks_mongo_store.query(criteria={"task_id": {"$in": task_ids_to_query}},
                                             properties={"task_id": 1, "dir_name": 1})
        emmet_restore_file = self.emmet_restore_file_path.open('w')
        emmet_restore_list: List = []
        for task in tasks:
            dir_name: str = task["dir_name"]
            start = dir_name.find("block_")
            dir_name = dir_name[start:]
            emmet_restore_file.write(dir_name + "\n")
            emmet_restore_list.append(dir_name)
        emmet_restore_file.close()
        return emmet_restore_list

    def organize_launchers(self, block_name: str, launcher_names: List[str]) -> List[str]:
        """
        turn [launcher-xxx, launcher-yyy, launcher-ccc] into
        [block_name/launcher-xxx, block_name/launcher-xxx/launcher-yyy, block_name/launcher-xxx/launcher-yyy/launcher-ccc]

        :param block_name: used to prepend block name
        :param launcher_names: list of launcher names
        :return: list of launcher names

        """
        result: List[str] = []
        prev_name = block_name
        for launcher_name in launcher_names:
            curr_name = prev_name + "/" + launcher_name
            result.append(curr_name)
            prev_name = curr_name
        return result

    def organize_path(self, paths: List[str]) -> Dict[str, List[str]]:
        result: Dict[str, List[str]] = dict()
        for path in paths:
            splitted: List[str] = path.split("/")
            block_name, launcher_names = splitted[0], splitted[1:]
            list_of_launchers = self.organize_launchers(block_name=block_name, launcher_names=launcher_names)
            if block_name in result:
                result[block_name].extend(list_of_launchers)
            else:
                result[block_name] = list_of_launchers
        return result

    def as_dict(self) -> dict:
        d = dict()
        d["limit"] = self.limit
        d["sources"] = [self.mongo_record_store.as_dict(), self.tasks_mongo_store.as_dict(),
                        self.materials_mongo_store.as_dict()]
        d["targets"] = [self.mongo_record_store.as_dict()]
        d["source_root_dir"] = self.source_root_dir.as_posix()
        d["mp_ids_to_upload"] = self.mp_ids_to_upload
        d["temporary_output_dir"] = self.output_dir.as_posix()
        return d

    @classmethod
    def from_dict(cls, d: dict):
        local_mongo_store: MongoStore = MongoStore.from_dict(d["sources"][0])
        tasks_mongo_store: MongoStore = MongoStore.from_dict(d["sources"][1])
        materials_mongo_store: MongoStore = MongoStore.from_dict(d["sources"][2])
        return GDriveBuilder(sources=[local_mongo_store, tasks_mongo_store, materials_mongo_store],
                             targets=local_mongo_store,
                             source_root_dir=Path(d["source_root_dir"]),
                             temporary_output_dir=Path(d["temporary_output_dir"]),
                             mp_ids_to_upload=d["mp_ids_to_upload"],
                             limit=d["limit"])
