#!/usr/bin/env python3.8

from __future__ import print_function
from genericpath import isdir, isfile
from glob import glob
from http.client import BAD_REQUEST
from multiprocessing.connection import wait
from time import sleep
from typing import List
import json
import io
import logging
import os.path
import concurrent.futures
import hashlib
from datetime import datetime


# gogole and http imports
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaIoBaseDownload
import googleapiclient
import google_auth_httplib2
import httplib2
from googleapiclient import discovery

# application imports
from gDrive_data_structures.data_types import *
from datastore.sqlite_store import *

# global variables
TOKEN_CACHE = '/home/ketchup/vscode/gdrive_client/tokens.json'
APP_CREDS = '/home/ketchup/vscode/gdrive_client/credentials.json'
LPORT = 34888
FOLDERS_CACHE_PATH = '/home/ketchup/vscode/gdrive_client/folders/'
DRIVE_CACHE_PATH = "~/gdrive/"
PAGE_SIZE = 50
FOLDER_FIELDS = 'files(*)'
FILE_FIELDS = 'files(*)'
EXPORT_NATIVE_DOCS = False
LOG_DIRECTORY = "log/"
TARGET_SCOPES = ["https://www.googleapis.com/auth/docs",
            "https://www.googleapis.com/auth/drive", 
            "https://www.googleapis.com/auth/activity"]
MEDIA_EXPORT_MATRIX = {
            "application/vnd.google-apps.document": { 
                    "targetMimeType": "application/vnd.openxmlformats-officedocument.wordprocessingml.document",    
                    "extension": ".docx"
            },   
            "application/vnd.google-apps.spreadsheet": {
                    "targetMimeType": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    "extension": ".xslx"
            }
}
ROOT_FOLDER_ID = ""
ROOT_FOLDER_OBJECT = None
MAX_THREADS = 1
CREDENTIALS = None
DATABASE_PATH = '/home/ketchup/vscode/gdrive_client/.metadata/md.db'
DATABASE = None
CHANGES_TOKEN = None
TYPE_GOOGLE_APPS = 'application/vnd.google-apps'
TYPE_GOOGLE_FOLDER = 'application/vnd.google-apps.folder'

# Create a new Http() object for every request
# https://googleapis.github.io/google-api-python-client/docs/thread_safety.html
# overrides the constructor of the http2 object 
def build_request(http, *args, **kwargs):
    new_http = google_auth_httplib2.AuthorizedHttp(CREDENTIALS, http=httplib2.Http())
    return googleapiclient.http.HttpRequest(new_http, *args, **kwargs)

# get the root folder
def getRootFolder(service) -> gFolder:
    logging.debug("fetching the root folder")
    rootFolder = None
    try:
        gServiceFiles = service.files()
        params = { "fileId": 'root'
        }       
        request = gServiceFiles.get(**params)
        rootFolderResult =request.execute()
        rootFolder = gFolder(rootFolderResult)
    except HttpError as err:
        logging.error("error fetching the root folder." + str(err))
        print(err)
    return rootFolder

# clear the local folder cache
def clearFolderCache(folder_path: str) -> bool:
    logging.debug("clearning the local folder cache")
    try:
        for f in os.listdir(folder_path):
            os.remove(os.path.join(dir, f))
        return True
    except Exception as err:
        logging.error("Failed to clear local cache. %s" % str(err))
        return False

# read the local folder metadata cache into memory
def readFolderCache(folder_path: str) -> List[dict]:
    logging.debug("loading local folder cache data into memory")
    try:
        driveFolders = []
        gFolderObjects = []
        files = os.listdir(folder_path)
        for f in files:
            with open(folder_path + f, 'r') as fileReader:
                fileData = json.loads(fileReader.read())
                folderObj = gFolder(fileData)
                if (f == '_root' or fileData['ownedByMe'] == True):
                    driveFolders.append(fileData)
                    gFolderObjects.append(folderObj)
                fileReader.close()
    except Exception as err:
        logging.error("failure in loading local folder cache." + str(err))
        print(str(err))

    rootFolder = list(filter(lambda rf: rf['id'] == ROOT_FOLDER_ID, driveFolders))
    gDriveRoot = gFolder(rootFolder[0])

    for fObj in gFolderObjects:
        for fSearch in gFolderObjects:
            if 'parents' in fSearch.properties.keys():
                if fObj.id in fSearch.properties['parents']:
                    fObj.add_child(fSearch)
    
    return gFolderObjects

# read the local folder metadata cache into memory
def read_folder_cache_from_db() -> List[dict]:
    logging.debug("loading folder cache objects into memory")
    try:
        gFolderObjects = []
        offset = 0
        while True:
            files, rowsReturned = DATABASE.fetch_gObjectSet(offset=offset, searchField = "mime_type",\
                                     searchCriteria="application/vnd.google-apps.folder")
            if rowsReturned == 0:
                break
            for f in files:
                if "ownedByMe" in f.properties.keys():
                    if (f.properties['ownedByMe'] == True):
                        gFolderObjects.append(f)
                if (f.id == ROOT_FOLDER_ID):
                    gFolderObjects.append(f)
                    rootFolder = f
            offset += rowsReturned
        
    except Exception as err:
        logging.error("failure in loading local folder cache." + str(err))
        print(str(err))

    #rootFolder = list(filter(lambda rf: rf['id'] == ROOT_FOLDER_ID, gFolderObjects))
    gDriveRoot = rootFolder
    global ROOT_FOLDER_OBJECT
    ROOT_FOLDER_OBJECT = rootFolder

    for fObj in gFolderObjects:
        for fSearch in gFolderObjects:
            if 'parents' in fSearch.properties.keys():
                if fObj.id in fSearch.properties['parents']:
                    fObj.add_child(fSearch)
    
    return gFolderObjects

def get_google_object(service, id:str):
    return_object = None
    try:
        gServiceFiles = service.files()
        params = { "fileId": id,
                    "fields": "*"
        }
        request = gServiceFiles.get(**params)
        object = request.execute()
        if object is not None:
            if object['mimeType'] == 'application/vnd.google-apps.folder':
                return_object = gFolder(object) 
            else:
                return_object = gFile(object)

    except HttpError as err:
        logging.error("Unable to fetch metadata from google drive for object id %s. %s" % (id, str(err)))
    except Exception as err:
        logging.error("Unable to fetch metadata from google drive for object id %s. %s" % (id, str(err)))
    
    
    return return_object
    

# print out the google drive folder tree (won't be used in production)
def printFolderTree(folders = None):
    # grab the root folder
    rootFolder = list(filter(lambda rf: rf.id == ROOT_FOLDER_ID, folders))
    #print(rootFolder[0]['name'])
    
    def printTree(parent, level=0):
        print("-" * level + parent.name)
        for child in parent.children:
            printTree(child, level+1)     
    
    #printTree(folders, rootFolder[0], 0)
    printTree(rootFolder[0], 0)
    return

# duplicate google drive directory structure to the local target directory
def copyFolderTree(rootFolder:gFolder, destPath:str):
    logging.debug("creating a copy of the remote folder '%s' locally.", rootFolder.name)
    if rootFolder is not None:
        try:
            if not os.path.exists(os.path.join(destPath, rootFolder.name)):
                os.mkdir(os.path.join(destPath, rootFolder.name))
            if rootFolder.children is not None:
                for child in rootFolder.children:
                    copyFolderTree(child, os.path.join(destPath, rootFolder.name))
        except Exception as err:
            logging.error("failure to copy folder tree." + str(err))
            print(err)
    else:
        return

# return a listing of files in a directory (non-recursive)
def listFileInDirectory(service, folder:gFolder, maxFiles = 1000) -> List[gFile]:
    logging.debug("listing files in %s directory", folder.name)
    files = []
    try:
        gServiceFiles = service.files()
        params = { "q": "mimeType!='application/vnd.google-apps.folder' and '" +
                    folder.id + "' in parents",
                    "pageSize": PAGE_SIZE, 
                    "fields": "nextPageToken," + FILE_FIELDS
        }
        request = gServiceFiles.list(**params)

        while (request is not None) and len(files) <= maxFiles:
            files_page = request.execute()
            fs = files_page.get('files', [])
            for f in fs:
                objFile = gFile(f)
                objFile.md5 = None
                files.append(objFile)

            request = gServiceFiles.list_next(request, files_page)
    except HttpError as err:
        logging.error("error listing files." + str(err))
        print(err)
    return files

# download all files in a folder (non-recursive)
def downloadFilesFromFolder(service, folder: gFolder, targetDir: str) -> bool:
    logging.debug("starting to download files from %s to %s" % (folder.name, targetDir))
    bResult = False
    try:
        files = listFileInDirectory(service, folder)

        # the google api module isn't thread safe, since it's based on http2 which also isn't thread safe
        # https://googleapis.github.io/google-api-python-client/docs/thread_safety.html
        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
            futures = []
            for f in files:
                if not "application/vnd.google-apps" in f.properties['mimeType']:
                    filePath = os.path.join(targetDir, folder.name, f.name)

                    # build a new http2 object to enable thread safety.  gets passed to each thread   
                    credentials = Credentials.from_authorized_user_file(TOKEN_CACHE, TARGET_SCOPES)
                    authorized_http = google_auth_httplib2.AuthorizedHttp(credentials, http=httplib2.Http())
                    service = discovery.build('drive', 'v3', requestBuilder=build_request, http=authorized_http)

                    # build new database object for multi-threading too
                    threadSafeDB = sqlite_store()
                    threadSafeDB.open(DATABASE_PATH)
                    
                    futures.append(executor.submit(
                            download_file, service, f, filePath, threadSafeDB
                        ))
            for future in concurrent.futures.as_completed(futures):
                result = future.result()
                print(str(result))
            #wait(futures) # want to make sure we don't start too many threads
    except Exception as err:
        logging.error("error downloading directory %s. %s." % (folder.name, str(err)))
        bResult = False
    return bResult

# download a single file (will be called multi-threaded)
def download_file(service, file: gFile, targetPath:str, threadSafeDB:sqlite_store = None):
    logging.debug("beginning to download file %s", file.name)
    sReturn = ""
    try:
        gServiceFiles = service.files()
        params = { "fileId": file.id,
                    "acknowledgeAbuse": True
        }
        request = gServiceFiles.get_media(**params)
        fileData = io.BytesIO()
        downloader = MediaIoBaseDownload(fileData, request)
        done = False
        print("downloading file %s." % targetPath)

        while done is False:
            status, done = downloader.next_chunk()
            #print(F'Download {int(status.progress() * 100)}.')

        with open(targetPath, "wb+") as f:
            f.write(fileData.getbuffer())

        file.localPath = targetPath
        file.md5 = hash_file(targetPath)

        if threadSafeDB is not None:
            threadSafeDB.insert_gObject(file=file)
        else:
            DATABASE.insert_gObject(file=file)

        fileSize = os.path.getsize(targetPath)
        sReturn = "file %s written %d byes." % (targetPath, fileSize)


    except HttpError as err:
        logging.error("error downloading file. %s" % str(err))
        print(err)
        sReturn = "file %s download failed with %s" % (targetPath, str(err))
    except Exception as err:
        logging.error("error downloading file. %s" % str(err))
        print(err)
        sReturn = "file %s download failed with %s" % (targetPath, str(err))
    return sReturn

def hash_file(filePath: str):
    hash  = hashlib.md5()
    fileBytes  = bytearray(128*1024)
    mv = memoryview(fileBytes)
    with open(filePath, 'rb', buffering=0) as f:
        while n := f.readinto(mv):
            hash.update(mv[:n])
    return hash.hexdigest()

# export a native google document format (can't be downloaded)
def export_native_file(service, file: gFile, targetPath: str)-> bool:
    logging.debug("exporting the native google application file %s.", file.name)
    bSuccess = False
    try:
        gServiceFiles = service.files()
        # get type of application
        targetMimeType = None
        if file.properties['mimeType'] in MEDIA_EXPORT_MATRIX.keys():
            targetMimeType = MEDIA_EXPORT_MATRIX[file.properties['mimeType']]["targetMimeType"]
            targetExtension = MEDIA_EXPORT_MATRIX[file.properties['mimeType']]["extension"]
            targetPath = targetPath + targetExtension
        if targetMimeType is None:
            return False
        params = { "fileId": file.id,
                    "mimeType": targetMimeType
        }
        request = gServiceFiles.export_media(**params)
        fileData = io.BytesIO()
        downloader = MediaIoBaseDownload(fileData, request)
        done = False
        while done is False:
            status, done = downloader.next_chunk()
            print(F'Download {int(status.progress() * 100)}.')

        with open(targetPath, "wb+") as f:
            f.write(fileData.getbuffer())

    except HttpError as err:
        logging.error("error exporting google application file. %s", str(err))
        print(err)
        bSuccess = False
    return bSuccess

def writeFolderCache(service, localCachePath:str = FOLDERS_CACHE_PATH):
    logging.debug("writing local folder cache to %s." % str(localCachePath))
    try:
        # get the root folder
        gServiceFiles = service.files()
        if not ROOT_FOLDER_OBJECT:
            request = gServiceFiles.get(fileId = 'root')
            rootFolder = request.execute()

        else:
            rootFolder = ROOT_FOLDER_OBJECT.properties
        
        fRootFolder = open(FOLDERS_CACHE_PATH + "_root", "w+")
        fRootFolder.write(json.dumps(rootFolder, indent = 4))
        fRootFolder.close()
        
        global ROOT_FOLDER_ID
        if ROOT_FOLDER_ID == '':
            ROOT_FOLDER_ID = rootFolder['id']
    
        #print('List files')
        
        pageToken = None
        params = { "q": "mimeType='application/vnd.google-apps.folder'",
                    "pageSize": PAGE_SIZE, 
                    "fields": "nextPageToken," + FOLDER_FIELDS
        }
        request = gServiceFiles.list(**params)

        while request is not None:
            files_page = request.execute()
            fs = files_page.get('files', [])
            for f in fs:
                #print(f)
                with open(FOLDERS_CACHE_PATH + f['id'], 'w+') as folder_data:
                    folderObj = gFolder(f)
                    DATABASE.insert_gObject(folder=folderObj)
                    if 'parents' in folderObj.properties.keys():
                        DATABASE.insert_parents(folderObj.id, folderObj.properties['parents'])
                    folder_data.write(json.dumps(f, indent=5))
                    folder_data.close()
        
            request = gServiceFiles.list_next(request, files_page)


    except HttpError as err:
        logging.error("error writing local folder cache. %s", str(err))
        print(err)

def get_full_folder_path(service, folder: gFolder)-> str:
    full_path = str(folder.name)
    try:
        if 'parents' in folder.properties.keys():   
            gServiceFiles = service.files()
            params = { "fileId": folder.properties['parents'][0], "fields": "parents, mimeType, id, name, ownedByMe"}
            request = gServiceFiles.get(**params)
            parent = request.execute()
            full_path = parent['name'] + "/" + full_path
            while 'parents' in parent.keys():
                params = { "fileId": parent['parents'][0], "fields": "parents, mimeType, id, name, ownedByMe"}
                request = gServiceFiles.get(**params)
                parent = request.execute()
                full_path = parent['name'] + "/" + full_path
            if parent['ownedByMe'] == False:
                # a folder shared outside of the current owner for the drive object.  
                # stick in the root folder
                full_path = "_shared_withme/" + full_path
   
        else:
            if folder.properties['ownedByMe'] == False:
                full_path = "_shared_withme/" + full_path
                
        
    except Exception as err:
        logging.error("Error getting full local path for folder id %s. %s" % (folder.id, str(err)))
        print(str(err))

    return full_path

# full sync down
def do_full_download(service, folder: gFolder, targetPath:str):
    logging.debug("starting full download from google drive to %s" % targetPath)
    try:
        downloadFilesFromFolder(service, folder, os.path.join(targetPath))
        if folder.children is not None:
            for child in folder.children:
                do_full_download(service, child, os.path.join(targetPath, folder.name))
        
    except Exception as err:
        logging.error("error writing local folder cache. %s" % str(err))
        print(str(err))
    
# gets the change token for changes in the drive since last sync
# https://developers.google.com/drive/api/guides/manage-changes
def get_drive_changes_token(service):
    logging.info("fetching the start changes token from Google Drive.")
    startToken = None
    try:
        response = service.changes().getStartPageToken().execute()
        startToken = response.get("startPageToken")
    except HttpError as err:
        logging.error("error getting changes start token. %s", str(err))
        print(err)
    except Exception as err:
        logging.error("error getting changes start token. %s", str(err))
        print(str(err))   
    
    return startToken

# get changes since the last change token fetch
# https://developers.google.com/drive/api/guides/manage-changes
def get_drive_changes(service, changeToken):
    changes = []
    try:
        while changeToken is not None:
            response = service.changes().list(pageToken=changeToken,
                                              spaces='drive').execute()
            for change in response.get('changes'):
                # Process change
                changes.append(change)
            if 'newStartPageToken' in response:
                # Last page, save this token for the next polling interval
                global CHANGES_TOKEN
                CHANGES_TOKEN = response.get('newStartPageToken')
            changeToken = response.get('nextPageToken')
    except HttpError as err:
        logging.error("error getting changes from Drive. %s", str(err))
        print(err)
    except Exception as err:
        logging.error("error getting changes from Drive. %s", str(err))
        print(str(err))   

    return changes

def scan_local_files(parentFolder:str):
    try:
        objects = os.listdir(parentFolder)
        for object in objects:
            object = os.path.join(parentFolder, object)
            if os.path.isfile(object):
                # multi-thread this too
                md5 = hash_file(object)
                DATABASE.insert_localFile(object, md5)
            elif os.path.isdir(object):
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
    
    logging.info("scanning google drive files, looking for files not in database")
    differences = []
    try:
        gServiceFiles = service.files()
        params = { "q": "'me' in owners",
                    "pageSize": PAGE_SIZE, 
                    "fields": "nextPageToken," + "files(id, name, mimeType, version, md5Checksum, parents, ownedByMe)"
        }
        request = gServiceFiles.list(**params)

        while (request is not None):
            files_page = request.execute()
            fs = files_page.get('files', [])
            for f in fs:
                dbFile = None
                rows = DATABASE.fetch_gObject(f['id'])
                if len(rows) > 0:
                    dbFile = rows[0]
                    
                if f['mimeType'] == TYPE_GOOGLE_FOLDER:
                    googleFolder = gFolder(f)
                    if dbFile is not None:
                        if dbFile.id != googleFolder.id and \
                                    dbFile.name != googleFolder.name:
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
                        if (dbFile.md5 != googleFile.properties['md5Checksum'] or \
                            dbFile.mimeType != googleFile.mimeType) and \
                            dbFile.properties['version'] < googleFile.properties['version']:
                                # fetch full metadata of the file
                                get_params = {"fileId": googleFile.id, "fields": "*"}
                                get_req = gServiceFiles.get(**get_params)
                                full_file = gFile(get_req.execute())
                                differences.append(full_file)
                    else:
                        if TYPE_GOOGLE_APPS not in googleFile.mimeType:
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
    localDrivePath = os.path.expanduser(DRIVE_CACHE_PATH)

    # loop through files on disk and find any that aren't in the db or different by hash
    # hash the local files and stick them into a temp table along with the md5 hash
    # then it's just sql from there

    logging.info("starting to scan local Google drive cache in %s" % localDrivePath)
    DATABASE.clear_local_files()
    scan_local_files(localDrivePath)

    # any files that are in the db but not on disk, purge the db records
    logging.info("purging database entries where objects aren't foudn on disk")
    DATABASE.delete_files_not_on_disk()

    return

# identify files on disk that are missing or different from the database

def main():

    """
    ********************************************************************
    Logging config and setup
    ********************************************************************
    """
    # try to initiate logging
    try:
        logDir = os.path.join(os.path.dirname(__file__), LOG_DIRECTORY)
        logFile = os.path.join(logDir, 'sync.log')
        if not os.path.exists(logDir):
            os.mkdir(logDir)
        logParams = {
            "filename": logFile,
            "filemode": 'w',
            "format": '%(asctime)s: %(name)s - %(levelname)s -[%(filename)s:%(lineno)s - %(funcName)s() ] - %(message)s',
            "datefmt": '%d-%b-%y %H:%M:%S',
            "level": logging.DEBUG
        }
        logging.basicConfig(**logParams)
        logging.info("Starting sync")

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
    global DATABASE
    DATABASE = sqlite_store()
    if not os.path.exists(DATABASE_PATH):
        DATABASE.create_db(dbPath='/home/ketchup/vscode/gdrive_client/.metadata/md.db')

    
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
    logging.debug("looking for the an existing token in" + TOKEN_CACHE)
    if os.path.exists(TOKEN_CACHE):
        creds = Credentials.from_authorized_user_file(TOKEN_CACHE, TARGET_SCOPES)
        with open(TOKEN_CACHE, 'r') as tokenFile:
            token = json.loads(tokenFile.read())
            if token['scopes'] != TARGET_SCOPES:
                logging.warning("token cache scopes are not valid, removing token")
                creds = None
                os.remove(TOKEN_CACHE)
            '''
            # not the actual refresh token expiration.  how do we get that?   
            tokenExpires = datetime.strptime(token['expiry'], "%Y-%m-%dT%H:%M:%S.%fZ")
            
            if datetime.now() > tokenExpires:
                logging.warning("token cache has expired.")
                creds = None
                os.remove(TOKEN_CACHE)
            '''

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
                flow = InstalledAppFlow.from_client_secrets_file(APP_CREDS, TARGET_SCOPES)
                creds = flow.run_local_server(port=0)
            # Save the credentials for the next run
            with open(TOKEN_CACHE, 'w+') as token:
                logging.debug("saving credentials to " + TOKEN_CACHE)
                token.write(creds.to_json())
        except HttpError as err:
            print(err)
    
    # cache tokens globally for multi-threading
    global CREDENTIALS
    CREDENTIALS = creds

    # build the drive API service
    service = build('drive', 'v3', credentials=creds)
    
    #populate root folder objects so that we can map the parents and children
    logging.debug("Fetching the root folder from Google drive.")    
    rootFolder = getRootFolder(service)
    global ROOT_FOLDER_ID 
    ROOT_FOLDER_ID = rootFolder.id
    global ROOT_FOLDER_OBJECT 
    ROOT_FOLDER_OBJECT = rootFolder

    DATABASE.open(dbPath=DATABASE_PATH)
    DATABASE.insert_gObject(folder=rootFolder) # won't insert a dupe


    #logging.info("clearing the local folder cache")
    #clearFolderCache(FOLDERS_CACHE_PATH)

    # fetch all the folders and structure from google drive
    #writeFolderCache(service) # only needed on first run to create the local folder tree

    # read the local cache and create linked folder tree objects
    # folders = readFolderCache(FOLDERS_CACHE_PATH)
    folders = read_folder_cache_from_db()


    #rootFolder = (list(filter(lambda rf: rf.id == ROOT_FOLDER_ID, folders)))[0]
    #ROOT_FOLDER_OBJECT = rootFolder
    #printFolderTree(folders)
    #copyFolderTree(rootFolder, '/home/ketchup/gdrive')


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
    # make sure local database is reconciled with what's on disk
    reconcile_local_files_with_db()

    # get google drive changes
    # this is a full scan which should only be run upon the initial start up. 
    # once the program is running, it will subscribe to change notifications  
    google_drive_changes = []
    #google_drive_changes = get_all_drive_files_not_in_db(service)


    # *** multi-thread this in the future


    # run throught the folders first and get those created
    if len(google_drive_changes) > 0:
        i = 0 
        for i in range(len(google_drive_changes)):
            if google_drive_changes[i].mimeType == 'application/vnd.google-apps.folder':
                for parent_id in google_drive_changes[i].properties['parents']:
                    parent_folder = get_google_object(service, parent_id)
                    full_path = os.path.join(DRIVE_CACHE_PATH, \
                            get_full_folder_path(service, parent_folder), \
                            google_drive_changes[i].name)
                    full_path = os.path.expanduser(full_path)
                    if not os.path.exists(full_path):
                        os.mkdir(os.path.expanduser(full_path))
                        DATABASE.insert_gObject(folder = google_drive_changes[i])
                    google_drive_changes.pop(i)

        # get and process the file changes after creating any folders
        for f in google_drive_changes:
            if TYPE_GOOGLE_APPS not in f.mimeType:
                if 'parents' not in f.properties.keys():
                    f.properties['parents'] = (ROOT_FOLDER_ID, )
                for parent_id in f.properties['parents']:
                    parent_folder = get_google_object(service, parent_id)
                    full_path = os.path.join(DRIVE_CACHE_PATH, get_full_folder_path(service, parent_folder), f.name)
                    full_path = os.path.expanduser(full_path)
                    if not os.path.exists(os.path.dirname(full_path)):
                        # need to make directories if we don't own the folders
                        os.makedirs(os.path.dirname(full_path), exist_ok=True)
                    download_file(service, f, full_path)
    
    # ******
    # ^^^^
    # there are some weird change sets things happening with the above.  need to figure out how filter those out or merge them
    # ******

    # start tracking changes
    global CHANGES_TOKEN
    logging.debug("fetching change token from google drive")
    CHANGES_TOKEN = get_drive_changes_token(service)

    
    # need start this in a separate worker thread i think.   
    try:
        while True:
            try:
                changes = get_drive_changes(service, CHANGES_TOKEN)
                logging.debug("retrieved %d changes from google drive" % len(changes))
                # process the folders first
                i = 0 
                for i in range(len(changes)):
                    full_path = ""
                    if changes[i]['removed'] == False:
                        if changes[i]['file']['mimeType'] == TYPE_GOOGLE_FOLDER:
                            folder = get_google_object(service, changes[i]['fileId']) 
                            if 'parents' in folder.properties.keys():
                                for parent_id in folder.properties['parents']:
                                    parent_folder = get_google_object(service, parent_id)
                                    full_path = os.path.join(DRIVE_CACHE_PATH, \
                                        get_full_folder_path(service, parent_folder), \
                                        folder.name)
                                    full_path = os.path.expanduser(full_path)
                                    if not os.path.exists(full_path):
                                        os.mkdir(os.path.expanduser(full_path))
                                        DATABASE.insert_gObject(folder = folder)
                            changes.pop(i)
                    else:
                        # handle removal of files and folders later
                        return
                    
                for change in changes:
                    if not change['removed'] == True:
                        if TYPE_GOOGLE_APPS not in change['file']['mimeType']:
                            file = get_google_object(service, changes[i]['fileId']) 
                            if 'parents' not in file.properties.keys():                    
                                file.properties['parents'] = (ROOT_FOLDER_ID, )
                            for parent_id in file.properties['parents']:
                                parent_folder = get_google_object(service, parent_id)
                                full_path = os.path.join(DRIVE_CACHE_PATH, get_full_folder_path(service, parent_folder), file.name)
                                full_path = os.path.expanduser(full_path)
                                if not os.path.exists(os.path.dirname(full_path)):
                                    # need to make directories if we don't own the folders
                                    os.makedirs(os.path.dirname(full_path), exist_ok=True)
                                download_file(service, file, full_path)

                    else:
                        # handle removes later
                        return
                
            except Exception as err:
                logging.error("error parsing change set. %s" % str(err))
            sleep(5)
    except KeyboardInterrupt:
        pass


    # max threads
    global MAX_THREADS
    MAX_THREADS = os.cpu_count() - 1
    logging.info("initializing %d threads", MAX_THREADS)


    #downloadFilesFromFolder(service, ROOT_FOLDER_OBJECT, '/home/ketchup/gdrive')
    
    # do full download.  only needed on first run
    #doFullDownload(service, ROOT_FOLDER_OBJECT, '/home/ketchup/gdrive')

    service.close()
    DATABASE.close()
    logging.info("Finished sync.") 
   

if __name__ == '__main__':
    main()