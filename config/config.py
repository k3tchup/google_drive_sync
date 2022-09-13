#!/usr/bin/env python

# config file for gdrive_sync
# global variables

#import preprocessing

import logging


LOG_LEVEL = logging.DEBUG
CONSOLE_LOG_LEVEL = logging.INFO
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
DATABASE_PATH = '/home/ketchup/vscode/gdrive_client/.metadata/md.db'