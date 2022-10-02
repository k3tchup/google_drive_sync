# system imports
import logging
import sys
import os
import io
import concurrent.futures
import shutil
from time import sleep
#import keyring

# google and http imports
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaIoBaseDownload
from googleapiclient.http import MediaFileUpload
import googleapiclient
import google_auth_httplib2
import httplib2
from googleapiclient import discovery

# application imports
current = os.path.dirname(os.path.realpath(__file__))
parent = os.path.dirname(current)
sys.path.append(parent)
from gDrive_data_structures.data_types import *
from datastore.sqlite_store import *
from local_modules.mods import *
from local_modules.keyring import *
#from local_modules.filewatcher import Change
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

def test_func():
    print("test function called")

def login_to_drive():
    logging.info("initializing application credentials")
    creds = None
    # The file token.json stores the user's access and refresh tokens, and is
    # created automatically when the authorization flow completes for the first
    # time.
    if cfg.USE_KEYRING == True:
        kr = Keyring()
    else:
        kr = None

    if cfg.USE_KEYRING == True:
        logging.debug("looking for and existing token in the OS keyring")
        try:
            tokenStr = kr.get_data("gdrive", "token")
            if tokenStr is not None and tokenStr != "":
                tokenStr = json.loads(tokenStr)
                if tokenStr['scopes'] != cfg.TARGET_SCOPES:
                    creds = None
                    kr.delete_data("gdrive", "token")
                else:
                    creds = Credentials.from_authorized_user_info(tokenStr, cfg.TARGET_SCOPES)
        except Exception as err:
            logging.error("Unable to fetch the oauth token from the OS keyring. %s" % str(err))
    else:
        logging.debug("looking for an existing token in" + cfg.TOKEN_CACHE)
        if os.path.exists(cfg.TOKEN_CACHE):
            creds = Credentials.from_authorized_user_file(cfg.TOKEN_CACHE, cfg.TARGET_SCOPES)
            with open(cfg.TOKEN_CACHE, 'r') as tokenFile:
                token = json.loads(tokenFile.read())
                if token['scopes'] != cfg.TARGET_SCOPES:
                    logging.warning("token cache scopes are not valid, removing token")
                    creds = None
                    os.remove(cfg.TOKEN_CACHE)

    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        logging.warning("valid credentials weren't found, initializing oauth consent from in default browser.")
        try:
            if creds and creds.expired and creds.refresh_token:
                try:
                    creds.refresh(Request())
                except HttpError as err:
                    logging.error("error logging in to google drive. %s" % str(err))
                except Exception as err:
                    # if refersh token expired, remove the token cache and rerun self
                    if 'invalid_grant: Token has been expired or revoked.' in err.args[0]:
                        logging.warning("oauth refresh token expired, clearing token cache.")
                        if cfg.USE_KEYRING == True:
                            kr.delete_data("gdrive", "token")
                        else:
                            os.remove(cfg.TOKEN_CACHE)
                        login_to_drive()
                        return
                    logging.error("error logging in to Google Drive. %s" % str(err))  
            else:
                flow = InstalledAppFlow.from_client_secrets_file(cfg.APP_CREDS, cfg.TARGET_SCOPES)
                creds = flow.run_local_server(port=0)
            # Save the credentials for the next run
            if cfg.USE_KEYRING == True:
                kr.store_data("gdrive", "token", creds.to_json())
            else:
                with open(cfg.TOKEN_CACHE, 'w+') as token:
                    logging.debug("saving credentials to " + cfg.TOKEN_CACHE)
                    token.write(creds.to_json())
        except HttpError as err:
            print(err)
    
    return creds

# Create a new Http() object for every request
# https://googleapis.github.io/google-api-python-client/docs/thread_safety.html
# overrides the constructor of the http2 object 
def build_request(http, *args, **kwargs):
    new_http = google_auth_httplib2.AuthorizedHttp(cfg.CREDENTIALS, http=httplib2.Http())
    return googleapiclient.http.HttpRequest(new_http, *args, **kwargs)

# get the root folder
def get_root_folder(service) -> gFolder:
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

# print out the google drive folder tree (won't be used in production)
def print_folder_tree(folders = None):
    # grab the root folder
    rootFolder = list(filter(lambda rf: rf.id == cfg.ROOT_FOLDER_ID, folders))
    #print(rootFolder[0]['name'])
    
    def printTree(parent, level=0):
        print("-" * level + parent.name)
        for child in parent.children:
            printTree(child, level+1)     
    
    #printTree(folders, rootFolder[0], 0)
    printTree(rootFolder[0], 0)
    return

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
        logging.info("downloading file %s." % targetPath)
        #print("downloading file %s." % targetPath)

        fileDir = os.path.dirname(targetPath)
        if not os.path.exists(fileDir):
            logging.debug("file's parent directory '%s' doesn't exist, creating." % fileDir)
            os.makedirs(os.path.expanduser(fileDir))

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
            cfg.DATABASE.insert_gObject(file=file)

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
def export_native_file(service, file: gFile, targetPath: str)-> bool:
    logging.debug("exporting the native google application file %s.", file.name)
    bSuccess = False
    try:
        gServiceFiles = service.files()
        # get type of application
        targetMimeType = None
        if file.properties['mimeType'] in cfg.MEDIA_EXPORT_MATRIX.keys():
            targetMimeType = cfg.MEDIA_EXPORT_MATRIX[file.properties['mimeType']]["targetMimeType"]
            targetExtension = cfg.MEDIA_EXPORT_MATRIX[file.properties['mimeType']]["extension"]
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

# return a listing of files in a directory (non-recursive)
def list_files_in_dir(service, folder:gFolder, maxFiles = 1000) -> List[gFile]:
    logging.debug("listing files in %s directory", folder.name)
    files = []
    try:
        gServiceFiles = service.files()
        params = { "q": "mimeType!='application/vnd.google-apps.folder' and '" +
                    folder.id + "' in parents",
                    "pageSize": cfg.PAGE_SIZE, 
                    "fields": "nextPageToken," + cfg.FILE_FIELDS
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
def download_files_from_folder(service, folder: gFolder, targetDir: str) -> bool:
    logging.debug("starting to download files from %s to %s" % (folder.name, targetDir))
    bResult = False
    try:
        files = list_files_in_dir(service, folder)

        # the google api module isn't thread safe, since it's based on http2 which also isn't thread safe
        # https://googleapis.github.io/google-api-python-client/docs/thread_safety.html
        with concurrent.futures.ThreadPoolExecutor(max_workers=cfg.MAX_THREADS) as executor:
            futures = []
            for f in files:
                if not "application/vnd.google-apps" in f.properties['mimeType']:
                    filePath = os.path.join(targetDir, folder.name, f.name)

                    # build a new http2 object to enable thread safety.  gets passed to each thread   
                    credentials = Credentials.from_authorized_user_file(cfg.TOKEN_CACHE, cfg.TARGET_SCOPES)
                    authorized_http = google_auth_httplib2.AuthorizedHttp(credentials, http=httplib2.Http())
                    service = discovery.build('drive', 'v3', requestBuilder=build_request, http=authorized_http)

                    # build new database object for multi-threading too
                    threadSafeDB = sqlite_store()
                    threadSafeDB.open(cfg.DATABASE_PATH)
                    
                    futures.append(executor.submit(
                            download_file, service, f, filePath, threadSafeDB
                        ))
            for future in concurrent.futures.as_completed(futures):
                result = future.result()
                logging.debug("Download result: %s" % str(result))
            #wait(futures) # want to make sure we don't start too many threads
    except Exception as err:
        logging.error("error downloading directory %s. %s." % (folder.name, str(err)))
        bResult = False
    return bResult


def write_folder_cache(service, localCachePath:str = cfg.FOLDERS_CACHE_PATH):
    logging.debug("writing local folder cache to %s." % str(localCachePath))
    try:
        # get the root folder
        gServiceFiles = service.files()
        if not cfg.ROOT_FOLDER_OBJECT:
            request = gServiceFiles.get(fileId = 'root')
            rootFolder = request.execute()

        else:
            rootFolder = cfg.ROOT_FOLDER_OBJECT.properties
        
        fRootFolder = open(cfg.FOLDERS_CACHE_PATH + "_root", "w+")
        fRootFolder.write(json.dumps(rootFolder, indent = 4))
        fRootFolder.close()
        
        #global ROOT_FOLDER_ID
        if cfg.ROOT_FOLDER_ID == '':
            cfg.ROOT_FOLDER_ID = rootFolder['id']
    
        #print('List files')
        
        pageToken = None
        params = { "q": "mimeType='application/vnd.google-apps.folder'",
                    "pageSize": cfg.PAGE_SIZE, 
                    "fields": "nextPageToken," + cfg.FOLDER_FIELDS
        }
        request = gServiceFiles.list(**params)

        while request is not None:
            files_page = request.execute()
            fs = files_page.get('files', [])
            for f in fs:
                #print(f)
                with open(cfg.FOLDERS_CACHE_PATH + f['id'], 'w+') as folder_data:
                    folderObj = gFolder(f)
                    folderObj.localPath = os.path.join(cfg.DRIVE_CACHE_PATH, get_full_folder_path(service, folderObj))
                    cfg.DATABASE.insert_gObject(folder=folderObj)
                    if 'parents' in folderObj.properties.keys():
                        cfg.DATABASE.insert_parents(folderObj.id, folderObj.properties['parents'])
                    folder_data.write(json.dumps(f, indent=5))
                    folder_data.close()
        
            request = gServiceFiles.list_next(request, files_page)


    except HttpError as err:
        logging.error("error writing local folder cache. %s", str(err))
        print(err)


# full sync down
def do_full_download(service, folder: gFolder, targetPath:str):
    logging.debug("starting full download from google drive to %s" % targetPath)
    try:
        download_files_from_folder(service, folder, os.path.join(targetPath))
        if folder.children is not None:
            for child in folder.children:
                do_full_download(service, child, os.path.join(targetPath, folder.name))
        
    except Exception as err:
        logging.error("error writing local folder cache. %s" % str(err))
        print(str(err))
    

# retrieve the metadata for Google object (file or folder)
def get_drive_object(service, id:str):
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
   

# create folder in Google Drive
def create_drive_folder(service, folderName:str, localPath:str, parentId:str=None) -> gFolder:
    folder = None
    try:

        if parentId is None or parentId == "":
            parentId = cfg.ROOT_FOLDER_ID
        file_metadata = {
            'name': folderName,
            'mimeType': 'application/vnd.google-apps.folder',
            'parents': parentId
        }

        logging.info("creating folder %s in Google Drive" % folderName)
        f = service.files().create(body=file_metadata, fields='*').execute()
        folder = gFolder(f)
        folder.localPath = localPath
        cfg.DATABASE.insert_gObject(folder=folder)

    except HttpError as err:
        logging.error("error creating Google Drive folder. %s" % str(err))
    except Exception as err:
        logging.error("error creating Google Drive folder. %s" % str(err))

    return folder

# create the entire folder tree, if any part doesn't exist
def create_drive_folder_tree(service, folderPath:str) -> gFolder:
    parentFolder = None
    try:
        if cfg.DRIVE_CACHE_PATH not in cfg.ROOT_FOLDER_OBJECT.localPath:
            raise "folder path isn't the defined drive cache path."
            return
        folderPath = folderPath.replace(cfg.ROOT_FOLDER_OBJECT.localPath + "/", "")
        folders = folderPath.split(os.sep)
        parent = cfg.ROOT_FOLDER_OBJECT
        currentFolder = cfg.ROOT_FOLDER_OBJECT.localPath
        for folder in folders:
            currentFolder = os.path.join(currentFolder, folder)
            dbFolders, c = cfg.DATABASE.fetch_gObjectSet(searchField="local_path", \
                            searchCriteria=currentFolder)
            if len(dbFolders) == 0:
                parent = create_drive_folder(service, folder, currentFolder, parent.id)
            else:
                parent = dbFolders[0]


        parentFolder = parent

    except HttpError as err:
        logging.error("error creating Google Drive folder tree. %s" % str(err))
    except Exception as err:
        logging.error("error creating Google Drive folder tree. %s" % str(err))
    return parentFolder



def upload_drive_file(service, filePath:str, parentId:str = None)-> gFile:
    file = None
    attempt = 1
    try:
        while attempt <= cfg.UPLOAD_RETRIES_MAX:
            # if file is under 5 mb perform a simple upload
            fileSize = os.path.getsize(filePath)
            fileHash = hash_file(filePath)
            if fileSize <= (5 * 1024 * 1024):
                f = upload_drive_file_simple(service, filePath, parentId)
            else:
                # we need to figure out how to resumable uploads at some point later
                f = upload_drive_file_simple(service, filePath, parentId)
            file = gFile(f)
            if fileHash != file.properties['md5Checksum']:
                logging.warning("File upload resulted in a hash mismatch.")
                # remove the file
                file = delete_drive_file(service, file)
                attempt += 1
            else:
                file.localPath = filePath
                file.md5 = fileHash
                cfg.DATABASE.insert_gObject(file=file)
                break
        if attempt == cfg.UPLOAD_RETRIES_MAX:
            logging.error("Exceeded max retries to upload file '%s'" % filePath)
    except HttpError as err:
        logging.error("error uploading file to Google Drive. %s" % str(err))
    except Exception as err:
        logging.error("error uploading file to Google Drive. %s" % str(err))
    return file

def upload_drive_file_simple(service, filePath:str, parentId:str=None)->gFile:
    file = None
    try:
        if parentId is None or parentId == "":
            parentId = cfg.ROOT_FOLDER_ID
        fileDir = os.path.dirname(filePath)
        fileName = os.path.basename(filePath)
        logging.info("performing simple upload of file '%s'" % filePath)
        file_metadata = {'name': fileName, 'parents': parentId}
        media = MediaFileUpload(filePath, resumable=True)
        file = service.files().create(body=file_metadata, media_body=media,
                                      fields='*').execute()
    except HttpError as err:
        logging.error("error downing a simple file upload to Google Drive. %s" % str(err))
    except Exception as err:
        logging.error("error downing a simple file upload to Google Drive. %s" % str(err))
    return file


# uploads new files that have been identified as missing from the cloud post reconciliation
def upload_new_local_files(service):
    logging.debug("starting to upload new local files to the cloud.")
    try:
        new_local_files, records = cfg.DATABASE.fetch_newLocalFiles()
        recordsParsed = 0
        while records > 0:
            for f in new_local_files:
                if cfg.ROOT_FOLDER_OBJECT.localPath in f.localPath:
                    if f.mimeType == cfg.TYPE_GOOGLE_FOLDER:
                        return
                    else:
                        parentFolder = os.path.dirname(f.localPath)
                        db_parentFolders, c = cfg.DATABASE.fetch_gObjectSet(searchField = "local_path", \
                                                    searchCriteria=parentFolder)
                        db_parentFolder = db_parentFolders[0]
                        if db_parentFolder is not None:
                            f.properties['parents'] = [db_parentFolder.id]
                        else:
                            parent = create_drive_folder_tree(service, parentFolder)
                            f.properties['parents'] = parent.id
                        #file = upload_drive_file(service, f.localPath, f.properties['parents'][0])
                        change = Change('created', f.localPath, None, 'file')
                        cfg.LOCAL_QUEUE.put(change)
                else:
                    logging.warning("skipping file '%s'. path not in local cache directory." % f.localPath)    
                recordsParsed += 1
            new_local_files, records = cfg.DATABASE.fetch_newLocalFiles(offset=recordsParsed)
    except Exception as err:
        logging.error("error uploading new files to the cloud. %s" % str(err))

def update_drive_file(service, file:gFile, localPath:str):
    logging.info("updating Google drive file %s." % file.name)
    updated_file = None
    try:
        media_body = MediaFileUpload(localPath, resumable=True)

        # Send the request to the API.
        updated_file = service.files().update(
            fileId=file.id,
            #body=file.properties,
            media_body=media_body).execute()
        # get the encriched full metadata
        updated_file = get_drive_object(service, updated_file['id'])
        updated_file.localPath = file.localPath
        updated_file.md5 = file.md5

        cfg.DATABASE.update_gObject(file=updated_file)

    except HttpError as err:
        logging.error("error updating Google drive file '%s'. %s" % (file.name, str(err)))
    except Exception as err:
        logging.error("error updating Google drive file '%s'. %s" % (file.name, str(err)))
    return updated_file 

def update_drive_files(service):
    logging.debug("starting to update changed local files to the cloud.")
    try:
        changed_local_files, records = cfg.DATABASE.fetch_changedLocalFiles()
        recordsParsed = 0
        while records > 0:
            for f in changed_local_files:
                if cfg.ROOT_FOLDER_OBJECT.localPath in f.localPath:
                    if f.mimeType == cfg.TYPE_GOOGLE_FOLDER:
                        return
                    else:
                        #parentFolder = os.path.dirname(f.localPath)
                        #db_parentFolders, c = cfg.DATABASE.fetch_gObjectSet(searchField = "local_path", \
                        #                            searchCriteria=parentFolder)
                        #db_parentFolder = db_parentFolders[0]
                        #if db_parentFolder is not None:
                        #    f.properties['parents'] = [db_parentFolder.id]
                        #else:
                        #    parent = create_drive_folder_tree(service, parentFolder)
                        #    f.properties['parents'] = parent.id
                        #file = update_drive_file(service, f, f.localPath)
                        change = Change('modified', f.localPath, None, 'file')
                        cfg.LOCAL_QUEUE.put(change)
                else:
                    logging.warning("skipping file '%s'. path not in local cache directory." % f.localPath)    
                recordsParsed += 1
            changed_local_files, records = cfg.DATABASE.fetch_newLocalFiles(offset=recordsParsed)
    except Exception as err:
        logging.error("error updating cloudfile '%s'. %s" % (f.name, str(err)))

def move_drive_file(service, file:gFile, newParent_id: str=None, newName:str = None) -> gFile:
    try:
        if file.properties['parents'][0] != newParent_id and newParent_id is not None:
            prev_parents = ','.join(file.properties['parents'])
            file = service.files().update(fileId=file.id, addParents=newParent_id,
                                        removeParents=prev_parents,
                                        fields='id, parents').execute()
            file = get_drive_object(service, file['id'])
            logging.info("moved file ID '%s' to new parent ID '%s'" % (file.id, newParent_id))
        else:
            if file.name != newName:
                file = service.files().update(fileId=file.id, body={'name': newName}).execute()
                file = get_drive_object(service, file['id'])
            else:
                logging.warning("Unable to process file '%s' move.  Can't parse the change." % file.id)
    except HttpError as err:
        logging.error("error moving file '%s' in Google Drive. %s" % (file.name, str(err)))
    except Exception as err:
        logging.error("error moving file '%s' in Google Drive. %s" % (file.name, str(err)))
    return file   

def delete_drive_file(service, file:gFile):
    #file = None
    try:
        file.properties['trashed'] = True
        gSerivceFiles = service.files()
        gSerivceFiles.delete(fileId = file.id).execute()
        logging.info("deleted Google Drive file with id %s" % file.id)
        #cfg.DATABASE.update_gObject(file=file)
        cfg.DATABASE.delete_gObject(id=file.id)

    except HttpError as err:
        logging.error("error deleteing file '%s' from Google Drive. %s" % (file.name, str(err)))
    except Exception as err:
        logging.error("error deleteing file '%s' from Google Drive. %s" % (file.name, str(err)))
    return file    

# region : Change tracking in Google drive

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
                #global CHANGES_TOKEN
                cfg.CHANGES_TOKEN = response.get('newStartPageToken')
            changeToken = response.get('nextPageToken')
    except HttpError as err:
        logging.error("error getting changes from Drive. %s", str(err))
        print(err)
    except Exception as err:
        logging.error("error getting changes from Drive. %s", str(err))
        print(str(err))   

    return changes

# handles any sort of change in a file in google drive (create, update, delete)
def handle_changed_file(service, file:gFile = None):
    try:
        parents = []
        if file is not None:
            # ******************************************
            #      create or update an existing file
            # ******************************************
            dbFiles = cfg.DATABASE.fetch_gObject(file.id)
            # **** handle file creation ****
            if len(dbFiles) > 1:
                logging.warn("file id %s has multiple entries in the database. skipping." % file.id)
            elif len(dbFiles) == 0:
                # **** handle new files from Google Drive ****
                logging.debug("file id %s isn't in the database, assuming a new object." % file.id)
                cfg.DATABASE.insert_gObject(file=file)
                if 'parents' in file.properties.keys():
                    for parent_id in file.properties['parents']:
                        parent_folder = get_drive_object(service, parent_id)
                        full_path = os.path.join(cfg.DRIVE_CACHE_PATH, \
                            get_full_folder_path(service, parent_folder), \
                            file.name)
                        full_path = os.path.expanduser(full_path)
                        cfg.LQUEUE_IGNORE.append(full_path)
                        download_file(service, file, full_path)
                        #cfg.LQUEUE_IGNORE.remove(full_path)
            else:
                # **** handle file updates ****
                dbFile  = dbFiles[0]
                if file.properties != dbFile.properties and int(file.properties['version']) > int(dbFile.properties['version']):
                    # if the md5 is different for the file, then we are going to remove the local version and re-download
                    logging.info("file id %s is newer in the cloud and has changes, processing." % file.id)
                    if (file.properties['trashed'] == False) and \
                                (file.properties['md5Checksum'] != dbFile.md5 or file.name != dbFile.name):
                        file.md5 = dbFile.md5 # we'll download it later if we need to
                        cfg.DATABASE.update_gObject(file=file)
                        try:
                            # delete the existing files and redownload for each instance of the file
                            if 'parents' in file.properties.keys():
                                for parent_id in file.properties['parents']:
                                    for db_parent_id in dbFile.properties['parents']:
                                        parent_folder = get_drive_object(service, parent_id)
                                        root_path = os.path.join(cfg.DRIVE_CACHE_PATH, \
                                            get_full_folder_path(service, parent_folder))
                                        full_path = os.path.join(root_path, file.name)
                                        full_path = os.path.expanduser(full_path)

                                        parent_folder = get_drive_object(service, db_parent_id)
                                        root_path_old = os.path.join(cfg.DRIVE_CACHE_PATH, \
                                            get_full_folder_path(service, parent_folder))
                                        full_path_old = os.path.join(root_path_old, dbFile.name)
                                        full_path_old = os.path.expanduser(full_path_old)


                                        # do the the redownload if the md5 doesn't match
                                        if file.properties['md5Checksum'] != dbFile.md5:
                                            logging.info("file id %s checksum is different and cloud version is newer, redownloading." % file.id)
                                            if os.path.exists(full_path):
                                                logging.info("removing outdated file '%s'." % full_path)
                                                os.remove(full_path)
                                            cfg.LQUEUE_IGNORE.append(full_path)
                                            download_file(service, file, full_path)
                                            #cfg.LQUEUE_IGNORE.remove(full_path)
                                        

                                        # do the rename
                                        if file.name != dbFile.name:
                                            if root_path_old == root_path:
                                                cfg.LQUEUE_IGNORE.append(full_path_old)
                                                cfg.LQUEUE_IGNORE.append(full_path)
                                                os.rename(full_path_old, full_path)
                                                #sleep(0.2) # give the Watchdog service time to catch up
                                                #cfg.LQUEUE_IGNORE.remove(full_path_old)
                                                #cfg.LQUEUE_IGNORE.remove(full_path)

                        except Exception as err:
                            logging.error("unable to update file id %s. %s" % (file.id, str(err)))

                    # ***** delete a local file ******
                    if file.properties['trashed'] == True:
                        file.md5 = dbFile.md5
                        cfg.DATABASE.update_gObject(file=file)
                        if 'parents' in file.properties.keys():
                            for parent_id in file.properties['parents']:
                                try:
                                    parent_folder = get_drive_object(service, parent_id)
                                    full_path = os.path.join(cfg.DRIVE_CACHE_PATH, \
                                        get_full_folder_path(service, parent_folder), \
                                        file.name)
                                    full_path = os.path.expanduser(full_path)
                                    if os.path.exists(full_path):
                                        logging.info("removing trashed file '%s'" % full_path)
                                        cfg.LQUEUE_IGNORE.append(full_path)
                                        os.remove(full_path)
                                        #sleep(0.2) # give the Watchdog service time to catch up
                                        #cfg.LQUEUE_IGNORE.remove(full_path)
                                except Exception as err:
                                    logging.error("unable to remove local file %s. %s" % (full_path, str(err)))

    except Exception as err:
        logging.error("error processing Google object change. %s" % str(err))
    except HttpError as err:
        logging.error("error processing Google object change. %s" % str(err))
    return 

# handles any sort of folder change in google drive (create, update, delete)
def handle_changed_folder(service, folder: gFolder = None):
    try:
        
        parents = []
                
        if folder is not None:
            # *************************************************************************
            #    create or update an existing folder
            # *************************************************************************
            dbFolders = cfg.DATABASE.fetch_gObject(folder.id)
            if len(dbFolders) > 1:
                logging.warn("folder id %s has multiple entries in the database. skipping." % folder.id)
            elif len(dbFolders) == 0:
                logging.debug("folder %s isn't in the database, assuming a new object." % folder.id)
                folder.localPath = os.path.join(cfg.DRIVE_CACHE_PATH, get_full_folder_path(service, folder))
                cfg.DATABASE.insert_gObject(folder=folder)
                if 'parents' in folder.properties.keys():
                    for parent_id in folder.properties['parents']:
                        parent_folder = get_drive_object(service, parent_id)
                        full_path = os.path.join(cfg.DRIVE_CACHE_PATH, \
                            get_full_folder_path(service, parent_folder), \
                            folder.name)
                        full_path = os.path.expanduser(full_path)
                        if not os.path.exists(full_path):
                            logging.info("creating new local folder '%s'" % full_path)
                            os.mkdir(os.path.expanduser(full_path))

            else:
                # if folder name is different, rename it.  if it's trashed, remove it.  only changes possible for folders
                dbFolder = dbFolders[0]
                if folder.properties != dbFolder.properties and int(folder.properties['version']) > int(dbFolder.properties['version']):
                    logging.info("folder id %s has a later version and different properties in Google Drive, applying changes" % folder.id)
                    # update the folder properties in the db
                    folder.localPath = os.path.join(cfg.DRIVE_CACHE_PATH, get_full_folder_path(service, folder))
                    cfg.DATABASE.update_gObject(folder=folder)
                    # **** rename the local folder(s) ****
                    if folder.name != dbFolder.name and folder.properties['trashed'] == False:
                        for parent_id in folder.properties['parents']:
                            for db_parent_id in dbFolder.properties['parents']:
                                parent_folder = get_drive_object(service, parent_id)
                                root_path_new = os.path.join(cfg.DRIVE_CACHE_PATH, \
                                    get_full_folder_path(service, parent_folder))
                                full_path_new = os.path.join(root_path_new, folder.name)
                                full_path_new = os.path.expanduser(full_path_new)

                                parent_folder = get_drive_object(service, db_parent_id)
                                root_path_old = os.path.join(cfg.DRIVE_CACHE_PATH, \
                                    get_full_folder_path(service, parent_folder))
                                full_path_old = os.path.join(root_path_old, dbFolder.name)
                                full_path_old = os.path.expanduser(full_path_old)

                                if root_path_old == root_path_new:
                                    os.rename(full_path_old, full_path_new)
                
                    # ***** delete a local folder ******
                    if folder.properties['trashed'] == True:
                        if 'parents' in folder.properties.keys():
                            for parent_id in folder.properties['parents']:
                                parent_folder = get_drive_object(service, parent_id)
                                full_path = os.path.join(cfg.DRIVE_CACHE_PATH, \
                                    get_full_folder_path(service, parent_folder), \
                                    folder.name)
                                full_path = os.path.expanduser(full_path)
                                if os.path.exists(full_path):
                                    logging.info("removing trashed directory '%s'" % full_path)
                                    shutil.rmtree(full_path)
                            
    except Exception as err:
        logging.error("error processing Google object change. %s" % str(err))
    except HttpError as err:
        logging.error("error processing Google object change. %s" % str(err))
    return




# endregion