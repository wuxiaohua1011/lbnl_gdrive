"""
This is a helper file that will fetch a list of newest launcher path that can be later used for fetching from HPSS
"""
from g_drive.gdrive_builder import GDriveBuilder
from maggma.stores.mongolike import MongoStore
from maggma.core.store import Sort
from pathlib import Path
import logging
import sys
import json
from pprint import pprint
from typing import List
import os

if __name__ == "__main__":
    output_file_path: Path = Path(os.getcwd()) / "files" / "data.txt"

    materials_mongo_store_path: Path = Path("./files/materials_mongo_store.json")
    loaded_info: dict = json.load(materials_mongo_store_path.open('r'))
    materials_mongo_store: MongoStore = MongoStore(database=loaded_info["database"],
                                                   collection_name=loaded_info['collection_name'],
                                                   host=loaded_info['host'],
                                                   port=int(loaded_info['port']),
                                                   username=loaded_info['username'],
                                                   password=loaded_info['password'])
    tasks_mongo_store_path: Path = Path("./files/task_mongo_store.json")
    loaded_info: dict = json.load(tasks_mongo_store_path.open('r'))
    tasks_mongo_store: MongoStore = MongoStore(database=loaded_info["database"],
                                               collection_name=loaded_info['collection_name'],
                                               host=loaded_info['host'],
                                               port=int(loaded_info['port']),
                                               username=loaded_info['username'],
                                               password=loaded_info['password'])
    materials_mongo_store.connect()
    tasks_mongo_store.connect()
    mp_ids_to_upload = ["mvc-2970", "mvc-66", "mvc-3716", "mvc-2513", "mvc-9059"]
    # materials = materials_mongo_store.query(criteria={"deprecated": False,
    #                                                   "task_id": {"$in": mp_ids_to_upload}},
    #                                         properties={"task_id": 1, "blessed_tasks": 1,
    #                                                     "last_updated": 1},
    #                                         sort={"last_updated": Sort.Descending},
    #                                         limit=1000)
    materials = materials_mongo_store.query(criteria={"deprecated": False},
                                            properties={"task_id": 1, "blessed_tasks": 1,
                                                        "last_updated": 1},
                                            sort={"last_updated": Sort.Descending},
                                            limit=1000)
    task_ids_to_query: List[str] = []
    for material in materials:
        print(material['task_id'])
        if "blessed_tasks" in material:
            blessed_tasks: dict = material["blessed_tasks"]
            task_ids_to_query.extend(list(blessed_tasks.values()))
        else:
            print(f"material [{material['task_id']}] does not have blessed tasks")

    # tasks = tasks_mongo_store.query(criteria={"task_id": {"$in": task_ids_to_query}},
    #                                 properties={"task_id": 1, "dir_name": 1})
    #
    # output_file = output_file_path.open('w')
    # for task in tasks:
    #     dir_name: str = task["dir_name"]
    #     start = dir_name.find("block_")
    #     dir_name = dir_name[start:]
    #     output_file.write(dir_name + "\n")
    #     print(dir_name)
