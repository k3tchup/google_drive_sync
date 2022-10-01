#!/usr/bin/env python3.8

from __future__ import print_function
from shelve import DbfilenameShelf
from time import sleep
from typing import List
import json
import logging
import os.path
from datetime import datetime

# google and http imports
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
#from googleapiclient.http import MediaIoBaseDownload
#import googleapiclient
#import google_auth_httplib2
#import httplib2
from googleapiclient import discovery

# application imports
from gDrive_data_structures.data_types import *
from datastore.sqlite_store import *
from config import config as cfg
from gDrive_modules.gDrive import *
from local_modules.mods import *
from local_modules.filewatcher import *

# scans all files in Google drive that aren't in the db.  that's our change set.
def get_gdrive_changes(service) -> List:
    # loop through the pages of files from google drive
    # return the md5Checksum property, along with name, id, mimeType, version, parents
    # compare files by id with the db.  look where the md5Checksum != md5 stored in the db
    # also look for files not in the db
    # important: if the file in google drive is a later version, it's authoritative
    # this will be the changes from the side of google drive
    
    logging.info("scanning google drive files, looking for files and folders that have changed.")
    differences = []
    try:
        gServiceFiles = service.files()
        params = { "q": "'me' in owners",
                    "pageSize": cfg.PAGE_SIZE, 
                    "fields": "nextPageToken," + "files(id, name, mimeType, version, md5Checksum, parents, ownedByMe)"
        }
        request = gServiceFiles.list(**params)

        while (request is not None):
            files_page = request.execute()
            fs = files_page.get('files', [])
            for f in fs:
                dbFile = None
                rows = cfg.DATABASE.fetch_gObject(f['id'])
                if len(rows) > 0:
                    dbFile = rows[0]
                    
                if f['mimeType'] == cfg.TYPE_GOOGLE_FOLDER:
                    googleFolder = gFolder(f)
                    if dbFile is not None and 'version' in dbFile.properties.keys():
                        # if (dbFile.id != googleFolder.id or \
                        #            dbFile.name != googleFolder.name) and \
                        #            dbFile.properties['version'] < googleFolder.properties['version']:
                        if (int(dbFile.properties['version']) < int(googleFolder.properties['version'])):
                            # fetch full metadata of the file
                            get_params = {"fileId": googleFolder.id, "fields": "*"}
                            get_req = gServiceFiles.get(**get_params)
                            full_folder = gFolder(get_req.execute())
                            full_folder.localPath = get_full_folder_path(service, full_folder)
                            differences.append(full_folder)
                    else:
                        get_params = {"fileId": googleFolder.id, "fields": "*"}
                        get_req = gServiceFiles.get(**get_params)
                        full_folder = gFolder(get_req.execute())
                        differences.append(full_folder)
                    
                else:
                    googleFile = gFile(f)
                    if dbFile is not None and 'version' in dbFile.properties.keys():
                        #if (dbFile.md5 != googleFile.properties['md5Checksum'] or \
                        #    dbFile.mimeType != googleFile.mimeType) and \
                        #    dbFile.properties['version'] < googleFile.properties['version']:
                        if (int(dbFile.properties['version']) < int(googleFile.properties['version'])):
                                # fetch full metadata of the file
                                get_params = {"fileId": googleFile.id, "fields": "*"}
                                get_req = gServiceFiles.get(**get_params)
                                full_file = gFile(get_req.execute())
                                differences.append(full_file)
                    else:
                        if cfg.TYPE_GOOGLE_APPS not in googleFile.mimeType:
                            get_params = {"fileId": googleFile.id, "fields": "*"}
                            get_req = gServiceFiles.get(**get_params)
                            full_file = gFile(get_req.execute())
                            differences.append(full_file)
            request = gServiceFiles.list_next(request, files_page)
    except HttpError as err:
        #exc_type, exc_obj, exc_tb = sys.exc_info()
        #fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        #logging.error(exc_type, fname, exc_tb.tb_lineno)
        logging.error("error scanning google drive files. %s" % str(err))
        print(err)
    except Exception as err:
        #exc_type, exc_obj, exc_tb = sys.exc_info()
        #fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        #logging.error(exc_type, fname, exc_tb.tb_lineno)
        logging.error("error scanning google drive files. %s" % str(err))
        print(err)
    return differences



# identify database entries of files not matching what's on disk.  delete the db entries.
def reconcile_local_files_with_db():
    localDrivePath = os.path.expanduser(cfg.DRIVE_CACHE_PATH)

    # loop through files on disk and find any that aren't in the db or different by hash
    # hash the local files and stick them into a temp table along with the md5 hash
    # then it's just sql from there

    logging.info("starting to scan local Google drive cache in %s" % localDrivePath)
    cfg.DATABASE.clear_local_files()
    scan_local_files_mt(localDrivePath)


    # any files that are in the db but not on disk, purge the db records
    # don't delete files that are marked as trashed, otherwise we'll download them again
    logging.info("purging database entries where objects aren't found on disk")
    cfg.DATABASE.delete_files_not_on_disk()

    # delete any files marked as 'trashed' in the db from disk
    logging.info("deleting local files that have been marked as trashed.")
    processed = 0
    while True:
        trashedObjects, c = cfg.DATABASE.fetch_deletedObjects(offset=processed)
        for file in trashedObjects:
            if file.mimeType != cfg.TYPE_GOOGLE_FOLDER and cfg.TYPE_GOOGLE_APPS not in file.mimeType:
                try:
                    if os.path.exists(file.localPath):
                        logging.info("removing trashed file '%s' from local filesystem." % file.localPath)
                        os.remove(file.localPath)
                except Exception as err:
                    logging.error("error removing trashed file %s. %s" % (file.localPath, str(err)))
            elif cfg.TYPE_GOOGLE_APPS in file.mimeType:
                logging.debug("ignoring google apps native doc with id %s" % file.id)
        # try to remove the directories (should be empty)
        for file in trashedObjects:
            if file.mimeType == cfg.TYPE_GOOGLE_FOLDER:
                try:
                    if os.path.exists(file.localPath):
                        if (len(os.listdir(file.localPath)) == 0):
                            logging.info("removing trashed folder '%s' from local filesystem." % file.localPath)
                            os.rmdir(file.localPath)
                        else:
                            logging.warning("folder %s isn't empty, skipping." % file.localPath)
                except Exception as err:
                    logging.error("error removing trashed folder %s. %s" % (file.localPath, str(err)))
        processed += c
        if c == 0:
            break
    
    return

def _runner():
    try:
        while True:
            time.sleep(cfg.POLLING_INTERVAL)
    except Exception as err:
        logging.error("Google Drive watcher stopped. %s" % str(err))

def _worker(lock=threading.Lock()):

    # needs it's onw service object for multithreading
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
        while True:
            try:
                with lock:
                    change = cfg.REMOTE_QUEUE.get()
                    if change.mimeType == cfg.TYPE_GOOGLE_FOLDER:
                        handle_changed_folder(service, change)
                    elif change.mimeType == cfg.TYPE_GOOGLE_APPS:
                        pass
                    else:
                        handle_changed_file(service, change)        
            except Exception as err:
                logging.error("Error handling queue task. %s" % str(err))
            finally:
                cfg.REMOTE_QUEUE.task_done()
                #while cfg.LOCAL_QUEUE.unfinished_tasks > 0:
                #    # wait for any resulting local operations are dequeued 
                #    sleep(1)
                #cfg.LQUEUE_IGNORE.remove(change.id)
    except Exception as err:
        logging.error("Error initializing remote queue worker. %s" % str(err))


def main():

    """
    ********************************************************************
    Logging config and setup
    ********************************************************************
    """
    # try to initiate logging
    try:
        logDir = os.path.join(os.path.dirname(__file__), cfg.LOG_DIRECTORY)
        logFile = os.path.join(logDir, 'sync.log')
        if not os.path.exists(logDir):
            os.mkdir(logDir)

        logFormatter = logging.Formatter("%(asctime)s: %(name)s - %(levelname)s -[%(filename)s:%(lineno)s - %(funcName)s() ] - %(message)s")
        logFormatter.datefmt = '%d-%b-%y %H:%M:%S'

        rootLogger = logging.getLogger()

        fileHandler = logging.FileHandler(logFile)
        fileHandler.setFormatter(logFormatter)
        rootLogger.addHandler(fileHandler)
        rootLogger.level = cfg.LOG_LEVEL

        consoleLogFormatter = logging.Formatter("%(asctime)s: %(levelname)s - %(message)s")
        consoleHandler = logging.StreamHandler()
        consoleHandler.setFormatter(consoleLogFormatter)
        consoleHandler.level = cfg.CONSOLE_LOG_LEVEL
        rootLogger.addHandler(consoleHandler)

        logging.info("Starting Google Drive sync")

    except Exception as err:
        print(str(err))
        raise Exception("unable to initialize logging")

    # fix up paths in the config
    cfg.DRIVE_CACHE_PATH = fixup_directory(cfg.DRIVE_CACHE_PATH)
    cfg.APP_CREDS = fixup_directory(cfg.APP_CREDS)
    cfg.TOKEN_CACHE = fixup_directory(cfg.TOKEN_CACHE)
    cfg.FOLDERS_CACHE_PATH = fixup_directory(cfg.FOLDERS_CACHE_PATH)
    cfg.DATABASE_PATH = fixup_directory(cfg.DATABASE_PATH)


    """
    ********************************************************************
    Initialize the local sqlite database.  used as metadata cache
    - stores file metadata of the stuff on disk
    ********************************************************************
    """
    logging.info("initialize local metadata store.")
    #global DATABASE
    cfg.DATABASE = sqlite_store()
    if not os.path.exists(cfg.DATABASE_PATH):
        cfg.DATABASE.create_db(dbPath=cfg.DATABASE_PATH)


    # max threads
    #global MAX_THREADS
    cfg.MAX_THREADS = os.cpu_count() - 1
    logging.info("Set up parallelism to %d threads", cfg.MAX_THREADS)
    
    """
    ********************************************************************
    Connect to Google drive via oauth. 
    - uses the authorization code flow
    - stores bearer and refresh token in a json file
    - supports granting consent to the scopes requested in a browser window (interactive)
    ********************************************************************
    """
    creds = login_to_drive()
    
    # cache tokens globally for multi-threading
    #global CREDENTIALS
    cfg.CREDENTIALS = creds

    # build the drive API service
    service = build('drive', 'v3', credentials=creds)
    
    #populate root folder objects so that we can map the parents and children
    logging.debug("Fetching the root folder from Google drive.")    
    rootFolder = get_root_folder(service)
    #global ROOT_FOLDER_ID 
    cfg.ROOT_FOLDER_ID = rootFolder.id
    #global ROOT_FOLDER_OBJECT 
    cfg.ROOT_FOLDER_OBJECT = rootFolder
    cfg.ROOT_FOLDER_OBJECT.localPath = cfg.DRIVE_CACHE_PATH + rootFolder.name

    cfg.DATABASE.open(dbPath=cfg.DATABASE_PATH)
    rootFolder.localPath = os.path.join(cfg.DRIVE_CACHE_PATH, rootFolder.name)
    cfg.DATABASE.insert_gObject(folder=rootFolder) # won't insert a dupe


    # initialize queueing
    cfg.LOCAL_QUEUE = queue.Queue(maxsize=0)
    cfg.REMOTE_QUEUE = queue.Queue(maxsize=0)

    # if this is the first run, skip the merge routine (local path is empty)
    if len(os.listdir(cfg.DRIVE_CACHE_PATH)) == 0:
        logging.info("Local cache folder is empty.  Skipping merge routines and downloading everything.")
        folders = read_folder_cache_from_db()
        if len(folders) == 0:
            write_folder_cache(service)
        do_full_download(service, cfg.ROOT_FOLDER_OBJECT, cfg.DRIVE_CACHE_PATH)


    # **************************************************************
    #  testing ground
    # **************************************************************
    #newFolder = create_drive_folder(service, "test5")
    #file = upload_file(service, '/home/ketchup/Downloads/user_agent_switcher-1.2.7.xpi', '1yTjqGApz4ClFazHwleeMf7pf3PXpozXK')  
       # **************************************************************
    #  end testing ground
    # **************************************************************

    # read the local cache and create linked folder tree objects
    folders = read_folder_cache_from_db()

    """
    ***********************************************************************************
    Do a full scan of Google drive for any files missing locally
    - this won't be necessary as we subscribe to the change notifications
    - only needed when starting up
    - first we purge any files from the db that aren't on disk
    - then we fetch the content from google drive and seeing what's not in the database
    - process the folders first so we have somewhere to download the files
    - then we process the missing files.
    ************************************************************************************

    """
    logging.info("looking for files changed since the last startup. this might take a bit of time.")
    # make sure local database is reconciled with what's on disk
    reconcile_local_files_with_db()

    # get google drive changes
    # this is a full scan which should only be run upon the initial start up. 
    # once the program is running, it will subscribe to change notifications  
    google_drive_changes = []
    google_drive_changes = get_gdrive_changes(service)


    # *** multi-thread this in the future

    logging.info("identified %d changes since the last run, reconciling." % len(google_drive_changes))
    # run throught the folders first and get those created
    if len(google_drive_changes) > 0:
        i = len(google_drive_changes) - 1
        for i in reversed(range(len(google_drive_changes))):
            if google_drive_changes[i].mimeType == 'application/vnd.google-apps.folder':
                handle_changed_folder(service, google_drive_changes[i])
                google_drive_changes.pop(i)
                #i-=1 # to avoid array index issues


        # get and process the file changes after creating any folders
        for f in google_drive_changes:
            if cfg.TYPE_GOOGLE_APPS not in f.mimeType:
                handle_changed_file(service, f)
    
    # ******
    # ^^^^
    # there are some weird change sets things happening with the above.  need to figure out how filter those out or merge them
    # ******

    # ****************************************************************************
    #          get local changes that are newer than what's in the cloud
    # ****************************************************************************
    # this gets done after the cloud sync is done and the database is current
    logging.info("looking for local files that need to be added or updated in Google Drive")
    upload_new_local_files(service)
    update_drive_files(service)


    # start tracking changes
    #global CHANGES_TOKEN
    logging.info("initial sync complete. watching for Google drive changes.")
    logging.debug("fetching change token from google drive")
    cfg.CHANGES_TOKEN = get_drive_changes_token(service)

    # start local watcher for any changes to files locally
    cfg.OBSERVER = Watcher(service)
    cfg.OBSERVER.run()

    # start remote watchers for any changs in Google Drive
    thread_runner = threading.Thread(target=_runner, daemon=True)
    threads = [threading.Thread(target=_worker, daemon=True)
                for _ in range(cfg.MAX_THREADS // 2)]
    for t in threads:
        t.start()
    thread_runner.start()
    
    try:
        while True:
            try:
                changes = get_drive_changes(service, cfg.CHANGES_TOKEN)
                logging.debug("retrieved %d changes from google drive" % len(changes))
                
                # grab full metadata for all the files first so that we can make informed decisions on the fly
                #enrichedChanges = []
                for change in changes:
                    gObject = get_drive_object(service, change['fileId'])
                    #enrichedChanges.append(gObject)
                    if gObject.id not in cfg.RQUEUE_IGNORE:
                        cfg.REMOTE_QUEUE.put(gObject)
                    else:
                        cfg.RQUEUE_IGNORE.remove(gObject.id)


               
                '''
                # handle removes first
                i = len(enrichedChanges) - 1
                for i in reversed(range(len(enrichedChanges))):
                    if enrichedChanges[i].properties['trashed']:
                        if enrichedChanges[i].mimeType != cfg.TYPE_GOOGLE_FOLDER and cfg.TYPE_GOOGLE_APPS not in enrichedChanges[i].mimeType:
                            handle_changed_file(service, enrichedChanges[i])
                            changes.pop(i)

                i = len(enrichedChanges) - 1
                for i in reversed(range(len(enrichedChanges))):
                    if enrichedChanges[i].properties['trashed']:
                        if enrichedChanges[i].mimeType == cfg.TYPE_GOOGLE_FOLDER:
                            handle_changed_folder(service, enrichedChanges[i])
                            changes.pop(i)

                
                # process the folders first for additions
                i = len(changes) -1
                for i in reversed(range(len(changes))):
                    full_path = ""
                    if changes[i]['removed'] == False:
                        if changes[i]['file']['mimeType'] == cfg.TYPE_GOOGLE_FOLDER:
                            folder = get_drive_object(service, changes[i]['fileId']) 
                            handle_changed_folder(service, folder)
                            changes.pop(i)
                    else:
                        # handle removal of files and folders later
                        return
                    
                for change in changes:
                    if not change['removed'] == True:
                        if cfg.TYPE_GOOGLE_APPS not in change['file']['mimeType']:
                            file = get_drive_object(service, changes[i]['fileId'])
                            handle_changed_file(service, file)

                    else:
                        # handle removes later
                        return
                '''
            except Exception as err:
                logging.error("error parsing change set. %s" % str(err))
            sleep(cfg.POLLING_INTERVAL)
    except KeyboardInterrupt:
        # need to stop Observer first
        pass

    cfg.OBSERVER.stop()

    for t in threads:
        t.join()
    thread_runner.join()

    service.close()
    cfg.DATABASE.close()
    logging.info("Finished sync.") 
   

if __name__ == '__main__':
    main()