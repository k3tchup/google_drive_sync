#!/usr/bin/env python3.8

# local modules for gsync client

import hashlib
import logging
import os
import sys
import concurrent

# application imports
current = os.path.dirname(os.path.realpath(__file__))
parent = os.path.dirname(current)
sys.path.append(parent)
from gDrive_data_structures.data_types import *
from datastore.sqlite_store import *
from config import config as cfg

# gets the md5 hash of a file
def hash_file(filePath: str):
    hash  = hashlib.md5()
    fileBytes  = bytearray(128*1024)
    mv = memoryview(fileBytes)
    with open(filePath, 'rb', buffering=0) as f:
        while n := f.readinto(mv):
            hash.update(mv[:n])
    return hash.hexdigest()


# clear the local folder cache
def clear_folder_cache(folder_path: str) -> bool:
    logging.debug("clearning the local folder cache")
    try:
        for f in os.listdir(folder_path):
            os.remove(os.path.join(dir, f))
        return True
    except Exception as err:
        logging.error("Failed to clear local cache. %s" % str(err))
        return False


# read the local folder metadata cache into memory
def read_folder_cache(folder_path: str) -> List[dict]:
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

    rootFolder = list(filter(lambda rf: rf['id'] == cfg.ROOT_FOLDER_ID, driveFolders))
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
            files, rowsReturned = cfg.DATABASE.fetch_gObjectSet(offset=offset, searchField = "mime_type",\
                                     searchCriteria="application/vnd.google-apps.folder")
            if rowsReturned == 0:
                break
            for f in files:
                if "ownedByMe" in f.properties.keys():
                    if (f.properties['ownedByMe'] == True):
                        gFolderObjects.append(f)
                if (f.id == cfg.ROOT_FOLDER_ID):
                    gFolderObjects.append(f)
                    rootFolder = f
            offset += rowsReturned
        
    except Exception as err:
        logging.error("failure in loading local folder cache." + str(err))
        print(str(err))

    #rootFolder = list(filter(lambda rf: rf['id'] == ROOT_FOLDER_ID, gFolderObjects))
    gDriveRoot = rootFolder
    #global ROOT_FOLDER_OBJECT
    cfg.ROOT_FOLDER_OBJECT = rootFolder

    for fObj in gFolderObjects:
        for fSearch in gFolderObjects:
            if 'parents' in fSearch.properties.keys():
                if fObj.id in fSearch.properties['parents']:
                    fObj.add_child(fSearch)
    
    return gFolderObjects

# multi-threaded function version
def scan_local_files_mt(parentFolder:str): 
    try:
        objects = os.listdir(parentFolder)
        with concurrent.futures.ThreadPoolExecutor(max_workers=cfg.MAX_THREADS) as executor:
            futures = []
            for object in objects:
                # build new database object for multi-threading too
                threadSafeDB = sqlite_store()
                threadSafeDB.open(cfg.DATABASE_PATH)

                object = os.path.join(parentFolder, object)
                # last modified time storec in epoch format in db for simplicity
                last_mod:float = os.path.getmtime(object)
                if os.path.isfile(object):
                    futures.append(executor.submit(
                        _do_scan_local_file, object, parentFolder, threadSafeDB
                    ))
                elif os.path.isdir(object):
                    cfg.DATABASE.insert_localFile(object, '', 'directory', last_mod)
                    scan_local_files(os.path.join(object))
                else:
                    return  
            for future in concurrent.futures.as_completed(futures):
                result = future.result() 

    except Exception as err:
        logging.error("error scanning local folder %s. %s", (parentFolder, str(err)))
        print(str(err))  

def _do_scan_local_file(object, parentFolder:str, threadSafeDB:sqlite_store = None):
    object = os.path.join(parentFolder, object)
    # last modified time storec in epoch format in db for simplicity
    last_mod:float = os.path.getmtime(object)
    if os.path.isfile(object):
        # multi-thread this too
        md5 = hash_file(object)
        if threadSafeDB is not None:
            threadSafeDB.insert_localFile(object, md5, "file", last_mod)
        else:
            cfg.DATABASE.insert_localFile(object, md5, "file", last_mod)
    else:
        return   

def scan_local_files(parentFolder:str):
    try:
        objects = os.listdir(parentFolder)
        for object in objects:
            object = os.path.join(parentFolder, object)
            # last modified time storec in epoch format in db for simplicity
            last_mod:float = os.path.getmtime(object)
            if os.path.isfile(object):
                # multi-thread this too
                md5 = hash_file(object)
                cfg.DATABASE.insert_localFile(object, md5, "file", last_mod)
            elif os.path.isdir(object):
                cfg.DATABASE.insert_localFile(object, '', 'directory', last_mod)
                scan_local_files(os.path.join(object))
            else:
                return   

    except Exception as err:
        logging.error("error scanning local folder %s. %s", (parentFolder, str(err)))
        print(str(err))  

# duplicate google drive directory structure to the local target directory
def copy_folder_tree(rootFolder:gFolder, destPath:str):
    logging.debug("creating a copy of the remote folder '%s' locally.", rootFolder.name)
    if rootFolder is not None:
        try:
            if not os.path.exists(os.path.join(destPath, rootFolder.name)):
                os.mkdir(os.path.join(destPath, rootFolder.name))
            if rootFolder.children is not None:
                for child in rootFolder.children:
                    copy_folder_tree(child, os.path.join(destPath, rootFolder.name))
        except Exception as err:
            logging.error("failure to copy folder tree." + str(err))
            print(err)
    else:
        return
