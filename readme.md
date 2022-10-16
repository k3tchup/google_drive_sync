# Google Drive Sync Client for Linux

> :warning: **This is a work in progress. There are still bugs and missing functionality.**

This is a Google Drive sync client for Linux.   It's written in Python and uses the Google Python APIs.  It's written as an oauth app.  

## Permissions / scopes required:

```
https://www.googleapis.com/auth/docs
https://www.googleapis.com/auth/drive 
https://www.googleapis.com/auth/activity
```

## Current functionality

* Integrates with Google API via oauth authorization code flow.  Caches access and refresh tokens.  Suppots consent via browser. https://developers.google.com/identity/protocols/oauth2#installed
* Sync Google drive down to a local folder
  * mapping folder parent-child relationships
  * downloading individual files
  * full sync down to local from Google drive
  * export of native Google Apps files, like Docs and Sheets.
  * hash verify the files
  * continuous monitoring for Drive for any changes and updating of local versions accordingly
* Upload local files to Google drive
  * track changed local files by hash and upload them to Drive. Also handles updates, moves, and deletes
* Sqlite database for metadata data storage 
* Storing access and refresh tokens in OS keyring
* Handles changes and tasks via queueing and multi-threading
* Handle incremental changes by subscribing to Google Drive change tokens
* Fetches files not owned by the Drive owner into a separate local folder

## Requirements

This code was written with Python 3.8.   Google Drive API requires Python 3.7 plus.   

The requirements.txt should list the remainder of the requirements. 

The OS keyring is used to stored the access and refresh tokens.  This must be set up ahead of time.   For WSL, install python3-keyrings.alt.

## To-do (work in progress)

[x] connect, manage scopes, tokens, credentials

[x] cache folder tree into local metadata cache

[x] build folde tree into linked objects (parents -> children)

[x] copy remove folder structure to local

[x] download non native apps files

[x] initial download of the entire drive

[x] checksum / hash verfication of downloaded files

[x] a local sqlite db to keep track of all the file versions and metadata?

[x] multi-threading for downloads

[x] don't bother going through the merge routines if the dest folder is empty, would save a bunch of time

[x] change handling currently redownloads the file and then deletes it.  this is ineffecient, optimize this.

[x] upload files

[x] delete files locally

[x] version handling so we don't transfer files we don't need to.   there are version numbers in the metadata

[x] subscribe to change notifications provided by Google drive

[x] logging 
- will need some improvement as we get going

[x] settings managed in a config file

[x] store the tokens in a keyring service (use a python module for this)

[x] delete files remotely

[x] resumable uploads (in case the connection fails)

[x] store the token.json contents in a key vault

[x] multi-thread for uploads

[x]] avoid double-processing changes (we download a file from Drive and detect that as a local change)

[x] multi-thread the merge functions

[x] implement a queueing service for dealing with the merges

[x] when fetching a file from local db, if multiple entries for the same path exist, pick the one that's the latest modified one and not deleted, if possible.   if all are deleted, then delete the file.  line 170 in filewatcher.py

[x] when downloading a file, need to set its modified time to what's in Drive (otherwise version comparison won't work)

[x] on first scan, detect deleted files in local cache and update Drive.  (increment version and mark them as trashed)

[x] for uploading changes, need to compare modified times before changing the remote files. entire local change handling needsa  refactor.

[x] change all silly while loops to queues and multi-thread that stuff

[x] handle file and directory moves

[x] add local file watcher to detect changes to the cache folder immediately

[x] move the local database cache and other variable data into the user's home directory

[x] better error handling

[ ] Address the issue with sqlite thread locking.  We sometimes run into the "recursive cursor" errors.  Look to lock the updates.

[ ] add retries via the queues.   add metadata for retries count and put the thing back on the queue

[ ] progress estimates for large downloads and uploads (both large by size and by number of files)

[ ] write export function to export google apps content.  need to adjust the mime type for the correct content.  examples below
```
'application/vnd.google-apps.document'
'application/vnd.google-apps.spreadsheet'
```
- mostly there
- still need to add more supported foramts. what are they?

[ ] figure out what to do with native docs we exported to docx/xslx/etc.  we don't want to put them back on the next sync.   do we want to flag them somewhere?  

[ ] is there a way to grab total size of drive from the API without recursing through all the files?

[ ] clear out trashed files after they expire on Google's side

[ ] there are instances when folders with the same name just inherit the same id when a few folder is crated with the same name.  need to debug this.

[ ] figure out why there are duplicate files with the same name and hash.  something to do with change sets.

[ ] bandwidth management
- the google modules call http2 classes.  can we just set limits there?

[ ] performance optimizations
- don't need to fetch full metadata all the time
- what can be cached locally?
- is there a way to optimize the get_media() function?

[ ] add vacuum command to the sqlite db routines to keep its size in check