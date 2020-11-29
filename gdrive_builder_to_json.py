from g_drive.gdrive_builder import GDriveBuilder
from maggma.stores.mongolike import MongoStore
from pathlib import Path
import logging
import sys
import json

local_mongo_store: MongoStore = MongoStore(database="gdrive", collection_name="gdrive")
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
mp_ids_to_upload = ["mvc-8151", "mvc-8154", "mp-606511", "mp-612200", "mp-612447"]
gdrive_builder = GDriveBuilder(sources=[local_mongo_store, tasks_mongo_store, materials_mongo_store],
                               targets=local_mongo_store,
                               source_root_dir=Path("/Volumes/KESU/lbnl_data/projects"),
                               temporary_output_dir=Path("/Volumes/KESU/lbnl_data/output"),
                               mp_ids_to_upload=mp_ids_to_upload)

from monty.serialization import dumpfn

# dumpfn(gdrive_builder, "./files/gdrive_builder.json")
dumpfn(gdrive_builder, "gdrive_builder.json")
