# adapted from: https://michaelcho.me/article/using-pythons-watchdog-to-monitor-changes-to-a-directory


#from concurrent.futures import thread
import time
import os
import sys
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
#import queue
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
    def __init__(self, change: str = "", src_object=None, dst_object=None, type=""):
        if change not in ['modified', 'created', 'deleted', 'moved', 'closed']:
            raise "Invalid change type '%s'" % change
        if type not in ['file', 'directory']:
            raise "Invalid change type '%s'" % type
        self.change_type = change
        self.object_type=type
        self.change_object = src_object
        self.dst_object = dst_object


class Watcher:

    def __init__(self, service):
        self.observer = Observer()
        self.service = service
        self.thread_runner = threading.Thread(target=self._runner, daemon=True)
        self.threads = [threading.Thread(target=self._worker, daemon=True)
                    for _ in range(cfg.MAX_THREADS // 2)]
        self.paused = False
        

    def run(self):
        event_handler = Handler(self.service)
        self.observer.schedule(event_handler, os.path.join(cfg.DRIVE_CACHE_PATH, cfg.ROOT_FOLDER_OBJECT.name), recursive=True)
        self.start_queue_processor()
        self.observer.start()
        
        self.thread_runner.start()
        #self.observer.join()
        #for t in self.threads:
        #    t.join()

    def pause(self):
        self.paused = True

    def resume(self):
        self.paused = False

    def _runner(self):
        try:
            while True:
                time.sleep(cfg.POLLING_INTERVAL)
        except Exception as err:
            self.observer.stop()
            logging.error("local file watcher stopped. %s" % str(err))

    def upload_drive_file(self, service, filePath: str, parentId: str = None) -> gFile:
        file = upload_drive_file(service, filePath, parentId)
        return file


    def stop(self):
        self.observer.stop()
        for t in self.threads:
            t.join()
        self.thread_runner.join()

    def _worker(self, lock=threading.Lock()):

        # needs it's own service object for multithreading
        try:
            if cfg.USE_KEYRING == True:
                kr = Keyring()
                tokenStr = kr.get_data("gdrive", "token")
                if tokenStr is not None and tokenStr != "":
                    tokenStr = json.loads(tokenStr)
                credentials = Credentials.from_authorized_user_info(tokenStr, cfg.TARGET_SCOPES)
            else:
                credentials = Credentials.from_authorized_user_file(cfg.TOKEN_CACHE, cfg.TARGET_SCOPES)
            authorized_http = google_auth_httplib2.AuthorizedHttp(credentials, http=httplib2.Http())
            service = discovery.build('drive', 'v3', requestBuilder=build_request, http=authorized_http)
            #service=self.service
            # give the remote queue a chance to clear before we initialize workers
            while cfg.REMOTE_QUEUE.qsize() > 0:
                sleep(1)

            while True:
                try:
                    with lock:
                        if not self.paused:
                            task = cfg.LOCAL_QUEUE.get()
                            if task.object_type == 'file':
                                if task.change_type == 'created':
                                    self.handle_file_create(service, task.change_object)
                                elif task.change_type == 'closed':
                                    #test_func2()
                                    #test_func()
                                    self.handle_file_change(service, task.change_object)
                                elif task.change_type == 'deleted':
                                    self.handle_file_delete(service, task.change_object)
                                elif task.change_type == 'moved':
                                    self.handle_file_move(service, task.change_object, task.dst_object)
                            else:
                                if task.change_type == 'moved':
                                    self.handle_file_move(service, task.change_object, task.dst_object)
                                elif task.change_type == 'created':
                                    self.handle_dir_create(service, task.change_object)
                                elif task.change_type == 'deleted':
                                    self.handle_file_delete(service, task.change_object)
                            
                except Exception as err:
                    logging.error("Error handling queue task. %s" % str(err))
                finally:
                    cfg.LOCAL_QUEUE.task_done()
        except Exception as err:
            logging.error("Error initializing local queue worker. %s" % str(err))

    def start_queue_processor(self):
        logging.info("Starting %d threads to handle local change queue." % (cfg.MAX_THREADS//2))
        for t in self.threads:
            t.start()

    def stop(self):
        if (self.observer.is_alive() == True):
            self.observer.stop

    def handle_file_create(self, service, filePath:str):
        try:
            # hash the file
            #md5 = hash_file(filePath)
            # get parent directory
            parentFolder = os.path.dirname(filePath)
            db_parentFolders, c = cfg.DATABASE.fetch_gObjectSet(searchField = "local_path", \
                                            searchCriteria=parentFolder)
            db_parentFolder = db_parentFolders[0]
            parent_id = None
            if db_parentFolder is not None:
                parent_id = [db_parentFolder.id]
            else:
                parent = create_drive_folder_tree(service, parentFolder)
                parent_id = parent.id
            file = self.upload_drive_file(service, filePath, parent_id)
            cfg.RQUEUE_IGNORE.append(file.id)
        except Exception as err:
            logging.error("error handling local file change. %s" % str(err))

    def handle_file_change(self, service, filePath:str):
        try:
            # hash the file
            md5 = hash_file(filePath)
            # find the file in the database
            dbFiles, c = cfg.DATABASE.fetch_gObjectSet(searchField = 'local_path', searchCriteria = filePath)
            if len(dbFiles) > 0:
                dbFile = dbFiles[0]
                # upload the file to Drive if needed
                if dbFile is not None:
                    # fetch the file metadata from Drive and only upload if our version is higher
                    upstreamFile = get_drive_object(service, dbFile.id)
                    if upstreamFile.properties['version'] < dbFile.properties['version']:
                        if dbFile.md5 != md5:
                            dbFile.md5 = md5
                            file = update_drive_file(service, dbFile, filePath)
                            cfg.RQUEUE_IGNORE.append(file.id)
                    else:
                        logging.error("Locally changed file '%s' is a lower version from the upstream file." % filePath)
            else:
                # treat it as create a file
                self.handle_file_create(service, filePath)
        except Exception as err:
            logging.error("error handling local file change. %s" % str(err))

    def handle_file_delete(self, service, filePath:str):
        try:
            dbFiles, c = cfg.DATABASE.fetch_gObjectSet(searchField = 'local_path', searchCriteria = filePath)
            if len(dbFiles) > 0:
                dbFile = dbFiles[0]
                # upload the file to Drive if needed
                if dbFile is not None:
                    delete_drive_file(service, dbFile)
                    cfg.RQUEUE_IGNORE.append(dbFile.id)
            else:
                logging.error("Deleted file '%s' wasn't found in metadata database." % filePath)
        except Exception as err:
            logging.error("error deleting local file. %s" % str(err))    

    def handle_file_move(self, service, srcPath:str, dstPath:str):
        try:
            dbFiles, c = cfg.DATABASE.fetch_gObjectSet(searchField = 'local_path', searchCriteria = srcPath)
            if len(dbFiles) > 0:
                dbFile = dbFiles[0]
                if dbFile is not None:
                    # get new parent
                    oldParentFolder = os.path.dirname(srcPath)
                    parentFolder = os.path.dirname(dstPath)
                    if oldParentFolder != parentFolder:
                        db_parentFolders, c = cfg.DATABASE.fetch_gObjectSet(searchField = "local_path", \
                                                        searchCriteria=parentFolder)
                        db_parentFolder = None
                        if c > 0:
                            db_parentFolder = db_parentFolders[0]
                        parent_id = None
                        if db_parentFolder is not None:
                            parent_id = db_parentFolder.id
                        else:
                            parent = create_drive_folder_tree(service, parentFolder)
                            parent_id = parent.id
                        move_drive_file(service=service, file=dbFile, newParent_id = parent_id, newName=None)
                        cfg.RQUEUE_IGNORE.append(dbFile.id)
                        dbFile.properties['parents'] = [parent_id]
                        dbFile.localPath = dstPath
                        if type(dbFile) == gFile:
                            cfg.DATABASE.update_gObject(file=dbFile)
                        else:
                            cfg.DATABASE.update_gObject(folder=dbFile)
                    else:
                        newFileName = os.path.basename(dstPath)
                        move_drive_file(service=service, file=dbFile, newParent_id=None, newName=newFileName)
                        cfg.RQUEUE_IGNORE.append(dbFile.id)
                        dbFile.name = newFileName
                        dbFile.properties['name'] = newFileName
                        dbFile.localPath = dstPath
                        if type(dbFile) == gFile:
                            cfg.DATABASE.update_gObject(file=dbFile)
                        else:
                            cfg.DATABASE.update_gObject(folder=dbFile)

            else:
                logging.error("Moved file '%s' wasn't found in metadata database." % srcPath)

        except Exception as err:
            logging.error("error moving file '%s'. %s" % (srcPath, str(err)))
    
    def handle_dir_change(self, service, srcPath: str):
        try:
            pass
        except Exception as err:
            logging.error("Error processing directory '%s' change. %s" % (srcPath, str(err)))

    def handle_dir_create(self, service, srcPath: str):
        try:
            # get parent directory
            parentFolder = os.path.dirname(srcPath)
            db_parentFolders, c = cfg.DATABASE.fetch_gObjectSet(searchField = "local_path", \
                                            searchCriteria=parentFolder)
            db_parentFolder = db_parentFolders[0]
            parent_id = None
            if db_parentFolder is not None:
                parent_id = [db_parentFolder.id]
            else:
                parent = create_drive_folder_tree(service, parentFolder)
                parent_id = parent.id
            folder = create_drive_folder(service, os.path.basename(srcPath), srcPath, parent_id)
        except Exception as err:
            logging.error("Error creating directory '%s'. %s" % (srcPath, str(err)))
    

class Handler(FileSystemEventHandler):

    def __init__(self, service=None):
        self.service = service
        self.lastDirectory = ""

    @staticmethod
    def on_any_event(event):
        if event.is_directory:     
            if event.event_type == 'created':
                if not event.src_path in cfg.LQUEUE_IGNORE:
                    logging.info("detected a new local directory '%s'" % event.src_path)
                    change = Change(event.event_type, event.src_path, None, 'directory')
                    cfg.LOCAL_QUEUE.put(change)
                else:
                    cfg.LQUEUE_IGNORE.remove(event.src_path)
            elif event.event_type == 'modified':
                logging.info("detected a modified local directory '%s'" % event.src_path)
                #change = Change(event.event_type, event.src_path, None, 'directory')
                #cfg.LOCAL_QUEUE.put(change)
            elif event.event_type == 'deleted':
                if not event.src_path in cfg.LQUEUE_IGNORE:
                    logging.info("detected a deleted local directory '%s'" % event.src_path)
                    change = Change(event.event_type, event.src_path, None, 'directory')
                    cfg.LOCAL_QUEUE.put(change)
                else:
                    cfg.LQUEUE_IGNORE.remove(event.src_path)
            elif event.event_type == 'moved':
                if not event.src_path in cfg.LQUEUE_IGNORE and event.dest_path not in cfg.LQUEUE_IGNORE:
                    logging.info("detected a moved local directory '%s'" % event.src_path)
                    Handler.lastDirectory = event.src_path # avoid dealing with child changes since they all get triggered as well
                    change = Change(event.event_type, event.src_path, event.dest_path, 'directory')
                    cfg.LOCAL_QUEUE.put(change)
                else:
                    cfg.LQUEUE_IGNORE.remove(event.src_path)
                    cfg.LQUEUE_IGNORE.remove(event.dest_path)
        else:
            change_dir = os.path.dirname(event.src_path)
            #if 'lastDirectory' in Handler.__dict__:
            #    if Handler.lastDirectory == change_dir:
            #        return
            #    else:
            #        Handler.lastDirectory = os.path.dirname(event.src_path)
            if event.event_type == 'created':
                logging.info("detected a new local file '%s'" % event.src_path)
            
            elif event.event_type == 'closed':
                # we'll handle file updates when the file is closed, otherwise, we are pushing incomplete changes for larger files.
                if event.src_path not in cfg.LQUEUE_IGNORE:
                    logging.info("detected changed local file '%s'" % event.src_path)
                    change = Change(event.event_type, event.src_path, None, 'file')
                    cfg.LOCAL_QUEUE.put(change)
                else:
                    cfg.LQUEUE_IGNORE.remove(event.src_path)

            elif event.event_type == 'moved':
                if event.src_path not in cfg.LQUEUE_IGNORE and event.dest_path not in cfg.LQUEUE_IGNORE:
                    logging.info("detected locally moved file '%s'" % event.src_path)
                    change=Change(event.event_type, event.src_path, event.dest_path, 'file')
                    cfg.LOCAL_QUEUE.put(change)
                else:
                    cfg.LQUEUE_IGNORE.remove(event.src_path)
                    cfg.LQUEUE_IGNORE.remove(event.dest_path)
                    
            elif event.event_type == 'deleted':
                if event.src_path not in cfg.LQUEUE_IGNORE:
                    logging.info("detected deleted local file '%s'" % event.src_path)
                    change = Change(event.event_type, event.src_path, None, 'file')
                    cfg.LOCAL_QUEUE.put(change)
                else:
                    cfg.LQUEUE_IGNORE.remove(event.src_path)
            else:
                logging.debug("unknown file watcher event. %s" % str(event.event_type))