# system imports
import logging
import sys
import os
import io
import concurrent.futures
import shutil

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
from config import config as cfg

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
                print(str(result))
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
   

# create folder in Google Drive
def create_drive_folder(service, folderName:str, parentId:str=None) -> gFolder:
    folder = None
    try:

        if parentId is None or parentId == "":
            parentId = cfg.ROOT_FOLDER_ID
        file_metadata = {
            'name': folderName,
            'mimeType': 'application/vnd.google-apps.folder',
            'parents': (parentId,)
        }

        logging.info("creating folder %s in Google Drive" % folderName)
        f = service.files().create(body=file_metadata, fields='*').execute()
        folder = gFolder(f)

    except HttpError as err:
        logging.error("error creating Google Drive folder. %s" % str(err))
    except Exception as err:
        logging.error("error creating Google Drive folder. %s" % str(err))

    return folder


def upload_file(service, filePath:str, parentId:str = None)-> gFile:
    file = None
    attempt = 1
    try:
        while attempt <= cfg.UPLOAD_RETRIES_MAX:
            # if file is under 5 mb perform a simple upload
            fileSize = os.path.getsize(filePath)
            fileHash = hash_file(filePath)
            if fileSize <= (5 * 1024 * 1024):
                f = upload_file_simple(service, filePath, parentId)
            else:
                # we need to figure out how to resumable uploads at some point later
                f = upload_file_simple(service, filePath, parentId)
            file = gFile(f)
            if fileHash != file.properties['md5Checksum']:
                logging.warning("File upload resulted in a hash mismatch.")
                # remove the file
                file = delete_gdrive_file(service, file)
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

def upload_file_simple(service, filePath:str, parentId:str=None)->gFile:
    file = None
    try:
        if parentId is None or parentId == "":
            parentId = cfg.ROOT_FOLDER_ID
        fileDir = os.path.dirname(filePath)
        fileName = os.path.basename(filePath)
        logging.info("performing simple upload of file '%s'" % filePath)
        file_metadata = {'name': fileName, 'parents': (parentId, )}
        media = MediaFileUpload(filePath)
        file = service.files().create(body=file_metadata, media_body=media,
                                      fields='*').execute()
    except HttpError as err:
        logging.error("error downing a simple file upload to Google Drive. %s" % str(err))
    except Exception as err:
        logging.error("error downing a simple file upload to Google Drive. %s" % str(err))
    return file


def delete_gdrive_file(service, file:gFile):
    file = None
    try:
        file.properties['trashed'] = True
        gSerivceFiles = service.files()
        params = { 'fileId': file.id, 'trashed': True}
        f = gSerivceFiles.update(**params).execute()
        file = gFile(f)
        cfg.DATABASE.update_gObject(file=file)

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
            if len(dbFiles) > 1:
                logging.warn("file id %s has multiple entries in the database. skipping." % file.id)
            elif len(dbFiles) == 0:
                # **** handle new files from Google Drive ****
                logging.debug("file id %s isn't in the database, assuming a new object." % file.id)
                cfg.DATABASE.insert_gObject(folder=file)
                if 'parents' in file.properties.keys():
                    for parent_id in file.properties['parents']:
                        parent_folder = get_google_object(service, parent_id)
                        full_path = os.path.join(cfg.DRIVE_CACHE_PATH, \
                            get_full_folder_path(service, parent_folder), \
                            file.name)
                        full_path = os.path.expanduser(full_path)
                        download_file(service, file, full_path)
            else:
                # **** handle file updates
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
                                        parent_folder = get_google_object(service, parent_id)
                                        root_path = os.path.join(cfg.DRIVE_CACHE_PATH, \
                                            get_full_folder_path(service, parent_folder))
                                        full_path = os.path.join(root_path, file.name)
                                        full_path = os.path.expanduser(full_path)

                                        parent_folder = get_google_object(service, db_parent_id)
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
                                            download_file(service, file, full_path)
                                        

                                        # do the rename
                                        if file.name != dbFile.name:
                                            if root_path_old == root_path:
                                                os.rename(full_path_old, full_path)

                        except Exception as err:
                            logging.error("unable to update file id %s. %s" % (file.id, str(err)))

                    # ***** delete a local file ******
                    if file.properties['trashed'] == True:
                        file.md5 = dbFile.md5
                        cfg.DATABASE.update_gObject(file=file)
                        if 'parents' in file.properties.keys():
                            for parent_id in file.properties['parents']:
                                try:
                                    parent_folder = get_google_object(service, parent_id)
                                    full_path = os.path.join(cfg.DRIVE_CACHE_PATH, \
                                        get_full_folder_path(service, parent_folder), \
                                        file.name)
                                    full_path = os.path.expanduser(full_path)
                                    if os.path.exists(full_path):
                                        logging.info("removing trashed file '%s'" % full_path)
                                        os.remove(full_path)
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
                cfg.DATABASE.insert_gObject(folder=folder)
                if 'parents' in folder.properties.keys():
                    for parent_id in folder.properties['parents']:
                        parent_folder = get_google_object(service, parent_id)
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
                    cfg.DATABASE.update_gObject(folder=folder)
                    # **** rename the local folder(s) ****
                    if folder.name != dbFolder.name:
                        for parent_id in folder.properties['parents']:
                            for db_parent_id in dbFolder.properties['parents']:
                                parent_folder = get_google_object(service, parent_id)
                                root_path_new = os.path.join(cfg.DRIVE_CACHE_PATH, \
                                    get_full_folder_path(service, parent_folder))
                                full_path_new = os.path.join(root_path_new, folder.name)
                                full_path_new = os.path.expanduser(full_path_new)

                                parent_folder = get_google_object(service, db_parent_id)
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
                                parent_folder = get_google_object(service, parent_id)
                                full_path = os.path.join(cfg.DRIVE_CACHE_PATH, \
                                    get_full_folder_path(service, parent_folder), \
                                    folder.name)
                                full_path = os.path.expanduser(full_path)
                                if os.path.exists(full_path):
                                    if len(os.listdir(full_path)) == 0:
                                        logging.info("removing trashed directory '%s'" % full_path)
                                        #os.rmdir(full_path)
                                        shutil.rmtree(full_path)
                                    else:
                                        logging.warning("unable to remove trashed dir '%s'. not empty" % full_path) 
                            
    except Exception as err:
        logging.error("error processing Google object change. %s" % str(err))
    except HttpError as err:
        logging.error("error processing Google object change. %s" % str(err))
    return




# endregion