# adapted from: https://michaelcho.me/article/using-pythons-watchdog-to-monitor-changes-to-a-directory

import time
import os
import sys
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

# application imports
current = os.path.dirname(os.path.realpath(__file__))
parent = os.path.dirname(current)
sys.path.append(parent)
from gDrive_data_structures.data_types import *
from gDrive_modules.gDrive import *
from datastore.sqlite_store import *
from local_modules.mods import *
from config import config as cfg

class Watcher:

    def __init__(self, service):
        self.observer = Observer()
        self.service = service

    def run(self):
        event_handler = Handler(self.service)
        self.observer.schedule(event_handler, os.path.join(cfg.DRIVE_CACHE_PATH, cfg.ROOT_FOLDER_OBJECT.name), recursive=True)
        self.observer.start()
        try:
            while True:
                time.sleep(cfg.POLLING_INTERVAL)
        except Exception as err:
            self.observer.stop()
            logging.error("local file watcher stopped. %s" % str(err))

        self.observer.join()

    def stop(self):
        if (self.observer.is_alive() == True):
            self.observer.stop


class Handler(FileSystemEventHandler):

    def __init__(self, service=None):
        self.service = service

    def handle_file_create(self, filePath:str):
        try:
            # hash the file
            md5 = hash_file(filePath)
            # get parent directory
            parentFolder = os.path.dirname(filePath)
            db_parentFolders, c = cfg.DATABASE.fetch_gObjectSet(searchField = "local_path", \
                                            searchCriteria=parentFolder)
            db_parentFolder = db_parentFolders[0]
            parent_id = None
            if db_parentFolder is not None:
                parent_id = [db_parentFolder.id]
            else:
                parent = create_drive_folder_tree(self.service, parentFolder)
                parent_id = parent.id
            file = upload_drive_file(self.service, filePath, parent_id)
        except Exception as err:
            logging.error("error handling local file change. %s" % str(err))

    def handle_file_change(self, filePath:str):
        try:
            # hash the file
            md5 = hash_file(filePath)
            # find the file in the database
            dbFiles, c = cfg.DATABASE.fetch_gObjectSet(searchField = 'local_path', searchCriteria = filePath)
            if dbFiles is not None:
                dbFile = dbFiles[0]
                # upload the file to Drive if needed
                if dbFile is not None:
                    if dbFile.md5 != md5:
                        dbFile.md5 = md5
                        # where do i get the service object from?  how do you pass args in this case?
                        update_drive_file(self.service, dbFile, filePath)
        except Exception as err:
            logging.error("error handling local file change. %s" % str(err))

    @staticmethod
    def on_any_event(event):
        if event.is_directory:
            return None
            # we'll handle this a bit later

        elif event.event_type == 'created':
            
            print("Received created event - %s." % event.src_path)

        elif event.event_type == 'modified':
            logging.info("detected changed local file '%s'" % event.src_path)
            Handler.handle_file_change(Handler, event.src_path)

        elif event.event_type == 'moved':
            i = 1
            # do something

        elif event.event_type == 'deleted':
            i = 1
            # do something
        else:
            logging.warning("unknown file watcher event. %s" % str(event.event_type))