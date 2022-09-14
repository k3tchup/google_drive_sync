#!/usr/bin/env python3.8

from __future__ import print_function
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

def scan_local_files(parentFolder:str):
    try:
        objects = os.listdir(parentFolder)
        for object in objects:
            object = os.path.join(parentFolder, object)
            if os.path.isfile(object):
                # multi-thread this too
                md5 = hash_file(object)
                cfg.DATABASE.insert_localFile(object, md5, "file")
            elif os.path.isdir(object):
                cfg.DATABASE.insert_localFile(object, '', 'directory')
                scan_local_files(os.path.join(object))
            else:
                return
        

    except Exception as err:
        logging.error("error scanning local folder %s. %s", (parentFolder, str(err)))
        print(str(err))  

# scans all files in Google drive that aren't in the db.  that's our change set.
def get_all_drive_files_not_in_db(service) -> List:
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
                    if dbFile is not None:
                        # if (dbFile.id != googleFolder.id or \
                        #            dbFile.name != googleFolder.name) and \
                        #            dbFile.properties['version'] < googleFolder.properties['version']:
                        if (int(dbFile.properties['version']) < int(googleFolder.properties['version'])):
                            # fetch full metadata of the file
                            get_params = {"fileId": googleFolder.id, "fields": "*"}
                            get_req = gServiceFiles.get(**get_params)
                            full_folder = gFolder(get_req.execute())
                            differences.append(full_folder)
                    else:
                        get_params = {"fileId": googleFolder.id, "fields": "*"}
                        get_req = gServiceFiles.get(**get_params)
                        full_folder = gFolder(get_req.execute())
                        differences.append(full_folder)
                    
                else:
                    googleFile = gFile(f)
                    if dbFile is not None:
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
        logging.error("error scanning google drive files. %s" % str(err))
        print(err)
    except Exception as err:
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
    scan_local_files(localDrivePath)


    # any files that are in the db but not on disk, purge the db records
    # don't delete files that are marked as trashed, otherwise we'll download them again
    logging.info("purging database entries where objects aren't found on disk")
    cfg.DATABASE.delete_files_not_on_disk()

    # delete any files marked as 'trashed' in the db from disk
    logging.info("deleting local files that have been marked as trashed.")
    toCount = 0
    while True:
        trashedObjects, toCount = cfg.DATABASE.fetch_deletedObjects(offset=toCount)
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
                        if (len(os.listdir(file.localPath) == 0)):
                            logging.info("removing trashed folder '%s' from local filesystem." % file.localPath)
                            os.rmdir(file.localPath)
                        else:
                            logging.warning("folder %s isn't empty, skipping." % file.localPath)
                except Exception as err:
                    logging.error("error removing trashed folder %s. %s" % (file.localPath, str(err)))
        if toCount == 0:
            break
    
    return


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
        cfg.DATABASE.create_db(dbPath='/home/ketchup/vscode/gdrive_client/.metadata/md.db')

    
    """
    ********************************************************************
    Connect to Google drive via oauth. 
    - uses the authorization code flow
    - stores bearer and refresh token in a json file
    - supports granting consent to the scopes requested in a browser window (interactive)
    ********************************************************************
    """

    logging.info("initializing application credentials")
    creds = None
    # The file token.json stores the user's access and refresh tokens, and is
    # created automatically when the authorization flow completes for the first
    # time.
    logging.debug("looking for the an existing token in" + cfg.TOKEN_CACHE)
    if os.path.exists(cfg.TOKEN_CACHE):
        creds = Credentials.from_authorized_user_file(cfg.TOKEN_CACHE, cfg.TARGET_SCOPES)
        with open(cfg.TOKEN_CACHE, 'r') as tokenFile:
            token = json.loads(tokenFile.read())
            if token['scopes'] != cfg.TARGET_SCOPES:
                logging.warning("token cache scopes are not valid, removing token")
                creds = None
                os.remove(cfg.TOKEN_CACHE)
            '''
            # not the actual refresh token expiration.  how do we get that?   
            tokenExpires = datetime.strptime(token['expiry'], "%Y-%m-%dT%H:%M:%S.%fZ")
            
            if datetime.now() > tokenExpires:
                logging.warning("token cache has expired.")
                creds = None
                os.remove(TOKEN_CACHE)
            '''
    # max threads
    #global MAX_THREADS
    cfg.MAX_THREADS = os.cpu_count() - 1
    logging.info("initializing %d threads", cfg.MAX_THREADS)

    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        logging.warning("valid credentials weren't found, initialize oauth consent")
        try:
            if creds and creds.expired and creds.refresh_token:
                try:
                    creds.refresh(Request())
                except HttpError as err:
                    logging.error("error logging in to google drive. %s" % str(err))
                    print(err)                
            else:
                flow = InstalledAppFlow.from_client_secrets_file(cfg.APP_CREDS, cfg.TARGET_SCOPES)
                creds = flow.run_local_server(port=0)
            # Save the credentials for the next run
            with open(cfg.TOKEN_CACHE, 'w+') as token:
                logging.debug("saving credentials to " + cfg.TOKEN_CACHE)
                token.write(creds.to_json())
        except HttpError as err:
            print(err)
    
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

    cfg.DATABASE.open(dbPath=cfg.DATABASE_PATH)
    cfg.DATABASE.insert_gObject(folder=rootFolder) # won't insert a dupe


    #logging.info("clearing the local folder cache")
    #clearFolderCache(FOLDERS_CACHE_PATH)

    # fetch all the folders and structure from google drive
    #writeFolderCache(service) # only needed on first run to create the local folder tree

    # **************************************************************
    #  testing ground
    # **************************************************************
    #newFolder = create_drive_folder(service, "test5")
    #file = upload_file(service, '/home/ketchup/Downloads/user_agent_switcher-1.2.7.xpi', '1yTjqGApz4ClFazHwleeMf7pf3PXpozXK')

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
    google_drive_changes = get_all_drive_files_not_in_db(service)


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

    # start tracking changes
    #global CHANGES_TOKEN
    logging.debug("fetching change token from google drive")
    cfg.CHANGES_TOKEN = get_drive_changes_token(service)

    
    # need start this in a separate worker thread i think.   
    #print("initial sync complete watching for Google drive changes")
    logging.info("initial sync complete. watching for Google drive changes.")
    try:
        while True:
            try:
                changes = get_drive_changes(service, cfg.CHANGES_TOKEN)
                logging.debug("retrieved %d changes from google drive" % len(changes))
                
                # grab full metadata for all the files first so that we can make informed decisions on the fly
                enrichedChanges = []
                for change in changes:
                    gObject = get_google_object(service, change['fileId'])
                    enrichedChanges.append(gObject)

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
                i = 0 
                for i in range(len(changes)):
                    full_path = ""
                    if changes[i]['removed'] == False:
                        if changes[i]['file']['mimeType'] == cfg.TYPE_GOOGLE_FOLDER:
                            folder = get_google_object(service, changes[i]['fileId']) 
                            handle_changed_folder(service, folder)
                            changes.pop(i)
                    else:
                        # handle removal of files and folders later
                        return
                    
                for change in changes:
                    if not change['removed'] == True:
                        if cfg.TYPE_GOOGLE_APPS not in change['file']['mimeType']:
                            file = get_google_object(service, changes[i]['fileId'])
                            handle_changed_file(service, file)

                    else:
                        # handle removes later
                        return
                
            except Exception as err:
                logging.error("error parsing change set. %s" % str(err))
            sleep(5)
    except KeyboardInterrupt:
        pass


    #downloadFilesFromFolder(service, ROOT_FOLDER_OBJECT, '/home/ketchup/gdrive')
    
    # do full download.  only needed on first run
    #doFullDownload(service, ROOT_FOLDER_OBJECT, '/home/ketchup/gdrive')

    service.close()
    cfg.DATABASE.close()
    logging.info("Finished sync.") 
   

if __name__ == '__main__':
    main()