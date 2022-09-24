#!/usr/bin/env python

# config file for gdrive_sync
# global variables

#import preprocessing

import logging


# config variables that can be changed
LOG_LEVEL = logging.DEBUG
CONSOLE_LOG_LEVEL = logging.INFO
TOKEN_CACHE = '~/.gdrive_sync/tokens.json'
APP_CREDS = '~/.gdrive_sync/credentials.json'
LPORT = 34888
FOLDERS_CACHE_PATH = '~/.gdrive_sync/folders/'
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
DATABASE_PATH = '~/.gdrive_sync/md.db'
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