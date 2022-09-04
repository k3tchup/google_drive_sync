from __future__ import print_function
from http.client import BAD_REQUEST
from multiprocessing.connection import wait
from typing import List
import json
import io
import logging
import os.path
import concurrent.futures

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

# global variables
TOKEN_CACHE = '/home/ketchup/vscode/gdrive_client/tokens.json'
APP_CREDS = '/home/ketchup/vscode/gdrive_client/credentials.json'
LPORT = 34888
FOLDERS_CACHE_PATH = '/home/ketchup/vscode/gdrive_client/folders/'
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
        logging.error("Failed to clear local cache." + str(err))
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
                    
                    futures.append(executor.submit(
                            downloadFile, service, f, filePath
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
def downloadFile(service, file: gFile, targetPath:str):
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

# export a native google document format (can't be downloaded)
def exportNativeFile(service, file: gFile, targetPath: str)-> bool:
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
                    folder_data.write(json.dumps(f, indent=5))
                    folder_data.close()
        
            request = gServiceFiles.list_next(request, files_page)


    except HttpError as err:
        logging.error("error writing local folder cache. %s", str(err))
        print(err)

# full sync down

def doFullDownload(service, folder: gFolder, targetPath:str):
    logging.debug("starting full download from google drive to %s" % targetPath)
    try:
        downloadFilesFromFolder(service, folder, os.path.join(targetPath))
        if folder.children is not None:
            for child in folder.children:
                doFullDownload(service, child, os.path.join(targetPath, folder.name))
        
    except Exception as err:
        logging.error("error writing local folder cache. %s" % str(err))
        print(str(err))
    



def main():

    # try to initiate logging
    try:
        logDir = os.path.join(os.path.dirname(__file__), LOG_DIRECTORY)
        logFile = os.path.join(logDir, 'sync.log')
        if not os.path.exists(logDir):
            os.mkdir(logDir)
        logParams = {
            "filename": logFile,
            "filemode": 'w',
            "format": '%(asctime)s: %(name)s - %(levelname)s - %(message)s',
            "datefmt": '%d-%b-%y %H:%M:%S',
            "level": logging.DEBUG
        }
        #logging.basicConfig(filename=logFile, filemode='w', format='%(name)s - %(levelname)s - %(message)s', level=logging.DEBUG)
        logging.basicConfig(**logParams)
        logging.info("Starting sync")

    except Exception as err:
        print(str(err))
        raise Exception("unable to initialize logging")

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

    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        logging.warning("valid credentials weren't found, initialize oauth consent")
        try:
            if creds and creds.expired and creds.refresh_token:
                creds.refresh(Request())
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

    service = build('drive', 'v3', credentials=creds)

    #populate root folder objects so that we can map the parents and children
    logging.debug("Fetching the root folder from Google drive.")    
    rootFolder = getRootFolder(service)
    global ROOT_FOLDER_ID 
    ROOT_FOLDER_ID = rootFolder.id
    global ROOT_FOLDER_OBJECT 
    ROOT_FOLDER_OBJECT = rootFolder

    logging.info("clearing the local folder cache")
    clearFolderCache(FOLDERS_CACHE_PATH)

    # fetch all the folders and structure from google drive
    writeFolderCache(service)

    # read the local cache and create linked folder tree objects
    folders = readFolderCache(FOLDERS_CACHE_PATH)
    rootFolder = (list(filter(lambda rf: rf.id == ROOT_FOLDER_ID, folders)))[0]
    ROOT_FOLDER_OBJECT = rootFolder
    #printFolderTree(folders)
    copyFolderTree(rootFolder, '/home/ketchup/gdrive')

    # max threads
    global MAX_THREADS
    MAX_THREADS = os.cpu_count() - 1
    logging.info("initializing %d threads", MAX_THREADS)


    #downloadFilesFromFolder(service, ROOT_FOLDER_OBJECT, '/home/ketchup/gdrive')
    doFullDownload(service, ROOT_FOLDER_OBJECT, '/home/ketchup/gdrive')

    service.close()
    logging.info("Finished sync.")

    
   

if __name__ == '__main__':
    main()