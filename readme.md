# Google Drive Sync Client for Linux

> :warning: **This is a work in progress with nothing functional yet.**

This is a Google Drive sync client for Linux.   It's written in Python and uses the Google Python APIs.  It's written as an oauth app.  

## Permissions / scopes required:

```
https://www.googleapis.com/auth/docs
https://www.googleapis.com/auth/drive 
https://www.googleapis.com/auth/activity
```

## To-do (work in progress)

[x] connect, manage scopes, tokens, credentials

[x] cache folder tree into local metadata cache

[x] build folde tree into linked objects (parents -> children)

[x] copy remove folder structure to local

[x] download non native apps file

[ ] write export function to export google apps content.  need to adjust the mime type for the correct content.  examples below
```
'application/vnd.google-apps.document'
'application/vnd.google-apps.spreadsheet'
```
- still need to add more supported foramts. what are they?

[ ] figure out what to do with native docs we exported to docx/xslx/etc.  we don't want to put them back on the next sync.   do we want to flag them somewhere? 

[ ] deal with downloads and uploads multi-threaded

[ ] upload files

[ ] version handling so we don't transfer files we don't need to.   there are version numbers in the metadata

[ ] subscribe to change notifications

[x] logging 
- will need some improvement as we get going

[ ] better error handling

[ ] settings managed in a json file