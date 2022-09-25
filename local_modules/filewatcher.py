# adapted from: https://michaelcho.me/article/using-pythons-watchdog-to-monitor-changes-to-a-directory

import time
import os
import sys
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
import queue
import threading

# application imports
current = os.path.dirname(os.path.realpath(__file__))
parent = os.path.dirname(current)
sys.path.append(parent)
from gDrive_data_structures.data_types import *
from gDrive_modules.gDrive import *
from datastore.sqlite_store import *
from local_modules.mods import *
from config import config as cfg

# data structure for queueing changes
class Change:
    def __init__(self, change: str = "", object=None, type=""):
        if change not in ['modified', 'created', 'deleted', 'moved', 'closed']:
            raise "Invalid change type '%s'" % change
        if type not in ['file', 'directory']:
            raise "Invalid change type '%s'" % type
        self.change_type = change
        self.object_type=type
        self.change_object = object


class Watcher:

    def __init__(self, service):
        self.observer = Observer()
        self.service = service
        self.threads = [threading.Thread(target=self._worker, daemon=True)
                    for _ in range(cfg.MAX_THREADS)]

    def run(self):
        event_handler = Handler(self.service)
        self.observer.schedule(event_handler, os.path.join(cfg.DRIVE_CACHE_PATH, cfg.ROOT_FOLDER_OBJECT.name), recursive=True)
        self.start_queue_processor()
        self.observer.start()
        
        try:
            while True:
                time.sleep(cfg.POLLING_INTERVAL)
        except Exception as err:
            self.observer.stop()
            logging.error("local file watcher stopped. %s" % str(err))

        self.observer.join()
        for t in self.threads:
            t.join()

    def _worker(self, lock=threading.Lock()):

        # needs it's onw service object for multithreading
        try:
            credentials = Credentials.from_authorized_user_file(cfg.TOKEN_CACHE, cfg.TARGET_SCOPES)
            authorized_http = google_auth_httplib2.AuthorizedHttp(credentials, http=httplib2.Http())
            service = discovery.build('drive', 'v3', requestBuilder=build_request, http=authorized_http)
            while True:
                try:
                    with lock:
                        task = cfg.LOCAL_QUEUE.get()
                        if task.object_type == 'file':
                            if task.change_type == 'created':
                                self.handle_file_create(task.change_object)
                            elif task.change_type == 'closed':
                                self.handle_file_change(task.change_object)
                except Exception as err:
                    logging.error("Error handling queue task. %s" % str(err))
                finally:
                    cfg.LOCAL_QUEUE.task_done()
        except Exception as err:
            logging.error("Error initializing local queue worker. %s" % str(err))

    def start_queue_processor(self):
        logging.info("Starting %d threads to handle local change queue." % cfg.MAX_THREADS)
        for t in self.threads:
            t.start()

    def stop(self):
        if (self.observer.is_alive() == True):
            self.observer.stop

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
            if len(dbFiles) > 0:
                dbFile = dbFiles[0]
                # upload the file to Drive if needed
                if dbFile is not None:
                    if dbFile.md5 != md5:
                        dbFile.md5 = md5
                        # where do i get the service object from?  how do you pass args in this case?
                        update_drive_file(self.service, dbFile, filePath)
            else:
                # treat it as create a file
                self.handle_file_create(filePath)
        except Exception as err:
            logging.error("error handling local file change. %s" % str(err))

class Handler(FileSystemEventHandler):

    def __init__(self, service=None):
        self.service = service

    @staticmethod
    def on_any_event(event):
        if event.is_directory:
            return None
            # we'll handle this a bit later

        elif event.event_type == 'created':
            logging.info("detected a new local file '%s'" % event.src_path)
            
        elif event.event_type == 'closed':
            # we'll handle file updates when the file is closed, otherwise, we are pushing incomplete changes for larger files.
            logging.info("detected changed local file '%s'" % event.src_path)
            change = Change(event.event_type, event.src_path, 'file')
            cfg.LOCAL_QUEUE.put(change)

        elif event.event_type == 'moved':
            i = 1
            # do something

        elif event.event_type == 'deleted':
            i = 1
            # do something
        else:
            logging.debug("unknown file watcher event. %s" % str(event.event_type))