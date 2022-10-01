#!/usr/bin/env python

# config file for gdrive_sync
# global variables

#import preprocessing

import logging
from typing import List


# config variables that can be changed
LOG_LEVEL = logging.DEBUG
CONSOLE_LOG_LEVEL = logging.INFO
LOG_DIRECTORY = "log/"

# tokens and auth
USE_KEYRING = True
TOKEN_CACHE = '~/.gdrive_sync/tokens.json'
APP_CREDS = '~/.gdrive_sync/credentials.json'
TARGET_SCOPES = ["https://www.googleapis.com/auth/docs",
            "https://www.googleapis.com/auth/drive", 
            "https://www.googleapis.com/auth/activity"]
LPORT = 34888

# metadata paths
FOLDERS_CACHE_PATH = '~/.gdrive_sync/folders/'
DATABASE_PATH = '~/.gdrive_sync/md.db'

# local google drive copy path
DRIVE_CACHE_PATH = "~/gdrive/"

# operation specific variables
PAGE_SIZE = 50
FOLDER_FIELDS = 'files(*)'
FILE_FIELDS = 'files(*)'
EXPORT_NATIVE_DOCS = False
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
UPLOAD_RETRIES_MAX = 3
POLLING_INTERVAL = 5 #seconds

# global variables that store dynamic values
ROOT_FOLDER_ID = ""
ROOT_FOLDER_OBJECT = None
MAX_THREADS = 1
CREDENTIALS = None
DATABASE = None
CHANGES_TOKEN = None
TYPE_GOOGLE_APPS = 'application/vnd.google-apps'
TYPE_GOOGLE_FOLDER = 'application/vnd.google-apps.folder'
LOCAL_QUEUE = None
REMOTE_QUEUE = None
OBSERVER = None
# ignore changes to these files (temporarily) while changes are being processed
# this is to avoid processing inotify changes for files we just downloaded and uploaded
LQUEUE_IGNORE = list()
RQUEUE_IGNORE = list()