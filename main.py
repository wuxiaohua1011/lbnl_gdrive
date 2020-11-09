from g_drive.gdrive_builder import GDriveBuilder
from maggma.stores.mongolike import MongoStore
from pathlib import Path
import logging
import sys
import json

if __name__ == "__main__":

    logging.getLogger("googleapiclient.discovery").setLevel(logging.ERROR)
    mongo_connection_file_path: Path = Path("./files/mongo_connection.json")
    loaded_info: dict = json.load(mongo_connection_file_path.open('r'))

    local_mongo_store: MongoStore = MongoStore(database="gdrive", collection_name="gdrive")
    tasks_mongo_store: MongoStore = MongoStore(database=loaded_info["database"],
                                               collection_name=loaded_info['collection_name'],
                                               host=loaded_info['host'],
                                               port=int(loaded_info['port']),
                                               username=loaded_info['username'],
                                               password=loaded_info['password'])
    mp_ids_to_upload = ["mvc-8151", "mp-606511", "mp-612200", "mp-612447"]
    gdrive_build = GDriveBuilder(sources=[local_mongo_store, tasks_mongo_store], targets=local_mongo_store,
                                 root_dir_path=Path("mwu"),
                                 mp_ids_to_upload=mp_ids_to_upload)
    gdrive_build.run()

