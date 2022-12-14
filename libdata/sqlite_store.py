
from gettext import find
import sqlite3
from sqlite3 import SQLITE_PRAGMA, Error
import logging
from typing import List, final
import json
import sys
import os
import threading
import datetime


current = os.path.dirname(os.path.realpath(__file__))
parent = os.path.dirname(current)
sys.path.append(parent)
from libdata.data_types import *
from config import config as cfg

class sqlite_store:
    def __init__(self):
        self.conn = None
        self.cursor = None
        self.lock = threading.Lock()
        return

    def __create_schema(self):
        logging.debug("creating database schema.")
        try:
            gObjectTable_sql = "CREATE TABLE IF NOT EXISTS gObjects (\
                                id nvarchar(100) PRIMARY KEY,\
                                name text NOT NULL,\
                                mime_type text NOT NULL,\
                                md5 text,\
                                local_path text,\
                                properties text NOT NULL\
                            );"
            parentChildrenTable_sql = "CREATE TABLE IF NOT EXISTS relationships (\
                                id integer PRIMARY KEY,\
                                parent_id nvarchar(100) NOT NULL,\
                                child_id nvarchar(100) NOT NULL,\
                                FOREIGN KEY (parent_id) REFERENCES gObjects (id),\
                                FOREIGN KEY (child_id) REFERENCES gObjects (id)\
                            );"

            localFiles_sql = "CREATE TABLE IF NOT EXISTS local_files (\
                                id integer PRIMARY KEY, \
                                path text NOT NULL, \
                                md5 text NOT NULL, \
                                mime_type text NOT NULL, \
                                last_mod real NOT NULL);"

            
            deleted_files_sql = "CREATE TABLE IF NOT EXISTS local_deleted (\
                                id integer PRIMARY KEY, \
                                deleted_id text NOT NULL);"

            self.cursor = self.conn.cursor()
            self.cursor.execute(gObjectTable_sql)
            self.conn.commit()
            self.cursor.execute(parentChildrenTable_sql)
            self.conn.commit()
            self.cursor.execute(localFiles_sql)
            self.conn.commit()
            self.cursor.execute(deleted_files_sql)
            self.conn.commit()

            # files that exist locally but not in drive (via the db)
            views_sql = "CREATE VIEW IF NOT EXISTS v_files_local_but_not_in_db \
                            AS \
                            SELECT local_files.id, \
                                local_files.path, \
                                local_files.md5, \
                                local_files.mime_type, \
                                local_files.last_mod \
                            FROM local_files \
                            LEFT JOIN gObjects \
                            ON local_files.path = gObjects.local_path \
                            AND local_files.md5 = gObjects.md5 \
                            WHERE (gObjects.md5 is null \
                            OR gObjects.local_path is null) \
                            AND local_files.mime_type != 'directory';"

            self.cursor.execute(views_sql)
            self.conn.commit()

            #files where local files are newer than the files in drive and hashes don't match
            views_sql = "CREATE VIEW IF NOT EXISTS v_files_modified_locally \
                            AS \
                            SELECT gObjects.id, \
                                gObjects.local_path, \
                                gObjects.md5, \
                                gObjects.mime_type, \
                                local_files.last_mod, \
                                gObjects.properties \
                            FROM local_files \
                            LEFT JOIN gObjects \
                            ON local_files.md5 != gObjects.md5 \
                            AND local_files.path = gObjects.local_path \
                            WHERE cast(strftime('%s', json_extract(gObjects.properties, '$.modifiedTime')) as integer) < \
                            cast(local_files.last_mod as integer);"
            self.cursor.execute(views_sql)
            self.conn.commit()

        except sqlite3.Error as error:
            logging.error("error creating database schema %s." % str(err))
        except Exception as err:
            logging.error("error creating database schema %s." % str(err))

    def clear_local_files(self):
        logging.debug("clearing the local_files table")
        try:
            truncateLocalFiles_sql = "DELETE FROM local_files;"
            self.cursor.execute(truncateLocalFiles_sql)
            self.conn.commit()
        
        except sqlite3.Error as e:
            logging.error("Unable to truncate the local_files table. %s" % str(e))
        except Exception as e:
            logging.error("Unable to truncate the local_files table. %s" % str(e))


    def create_db(self, dbPath: str):
        logging.debug("creating database %s" % dbPath)
        try:
            self.conn = sqlite3.connect(dbPath, check_same_thread=False)
            self.__create_schema()
        except sqlite3.Error as error:
            logging.error("unable to create database %s. %s" % (dbPath, str(e)))
        except Exception as e:
            logging.error("unable to create databaase %s. %s" % (dbPath, str(e)))

    def fetch_gObject(self, id: str):
        logging.debug("fetching database object with id %s" % id)
        objects = []
        try:
            self.lock.acquire(True)
            fetchObject_sql = "SELECT id, name, mime_type, properties, md5, local_path FROM gObjects WHERE id = ?;"
            sqlParams = (id, )
            self.cursor.execute(fetchObject_sql, sqlParams)
            rows = self.cursor.fetchall()
        
            for row in rows:
                if row[2] == 'application/vnd.google-apps.folder':
                    folder = gFolder(json.loads(row[3]))
                    folder.localPath = row[5]
                    objects.append(folder)
                else:
                    file = gFile(json.loads(row[3]))
                    file.md5 = row[4]
                    file.localPath = row[5]
                    objects.append(file)

        except sqlite3.Error as e:
            logging.error("Unable to fetch object id %s. %s" % (id, str(e)))
        except Exception as e:
            logging.error("Unable to fetch object id %s. %s" % (id, str(e)))
        finally:
            self.lock.release()

        return objects

    def fetch_parents(self, id: str):
        parents = []
        try:
            fetchObject_sql = "SELECT * FROM relationships WHERE child_id = ?;"
            sqlParams = (id, ) 

            self.cursor.execute(fetchObject_sql, sqlParams)
            rows = self.cursor.fetchall()
        
            for row in rows:
                parents.append(row[1])
                

        except sqlite3.Error as e:
            logging.error("Unable to fetch relationships for id %s. %s" % (id, str(e)))
        except Exception as e:
            logging.error("Unable to fetch relationships for id %s. %s" % (id, str(e)))

        return parents

    def fetch_newLocalFiles(self, pageSize:int = 100, offset:int = 0):
        logging.debug("fetching files that exist locally but not in the cloud.")
        results = []
        totalFetched = 0
        try:
            fetch_sql = "SELECT id, \
                            path, \
                            md5, \
                            mime_type, \
                            last_mod \
                        FROM v_files_local_but_not_in_db LIMIT ? OFFSET ?;"
            sqlParams = (pageSize, offset)
            self.cursor.execute(fetch_sql, sqlParams)
            rows = self.cursor.fetchall()
            for row in rows:
                try:
                    if row[3] == 'directory':
                        temp = {
                            "id": "_local_" + str(row[0]),
                            "name": os.path.basename(row[1]),
                            "mimeType": cfg.TYPE_GOOGLE_FOLDER,
                            "properties": { 'modifiedTime': str(datetime.datetime.fromtimestamp(row[4]).strftime('%Y-%m-%dT%H:%M:%S.%sZ')) }
                        }
                        f = gFile(temp)
                        f.localPath = row[1]
                    else:
                        # build a temp object struct
                        temp = {
                            "id": "_local_" + str(row[0]),
                            "name": os.path.basename(row[1]),
                            "mimeType": "",
                            "properties": { 'modifiedTime': str(datetime.datetime.fromtimestamp(row[4]).strftime('%Y-%m-%dT%H:%M:%S.%sZ')) }
                        }
                        f = gFile(temp)
                        f.localPath = row[1]
                        f.md5 = row[2]
                    results.append(f)
                    totalFetched +=1
                except Exception as err:
                    logging.error("unable to fetch db object and construct the appropriate structure. %s" % str(err))


        except sqlite3.Error as e:
            logging.error("Unable to fetch records. %s" % str(e))
        except Exception as e:
            logging.error("Unable to fetch records. %s" % str(e))
        
        return totalFetched, results


    def mark_changedLocalFiles(self):
        logging.debug("Incrementing the version on locally changed files.")
        try:
            # start a transaction for atomicity
            self.cursor.execute("BEGIN")

            # increment the version of the file
            update_sql = "UPDATE gObjects \
                            SET properties = json_patch(properties, \
                                '{\"version\":' || (json_extract(properties, '$.version')+1) || '}') \
                            WHERE id IN ( \
                            SELECT id FROM v_files_modified_locally);"

            self.cursor.execute(update_sql)
            self.conn.commit()

        except sqlite3.Error as e:
            logging.error("Unable to increment version on changed files. %s" % str(e))
            self.cursor.execute("ROLLBACK;")
        except Exception as e:
            logging.error("Unable to increment version on changed files. %s" % str(e))
            self.cursor.execute("ROLLBACK;")



    def fetch_changedLocalFiles(self, pageSize:int = 100, offset:int = 0):
        logging.debug("fetching local files that are newer vs what's in the cloud.")
        results = []
        totalFetched = 0
        try:
            fetch_sql = "SELECT id, \
                            local_path, \
                            md5, \
                            mime_type, \
                            last_mod, \
                            properties \
                        FROM v_files_modified_locally LIMIT ? OFFSET ?;"
            sqlParams = (pageSize, offset)
            self.cursor.execute(fetch_sql, sqlParams)
            rows = self.cursor.fetchall()
            for row in rows:
                try:
                    if row[3] == cfg.TYPE_GOOGLE_FOLDER:
                        temp = {
                            "id": str(row[0]),
                            "name": os.path.basename(row[1]),
                            "mimeType": cfg.TYPE_GOOGLE_FOLDER
                            #"properties": { 'modifiedTime': str(datetime.datetime.fromtimestamp(row[4]).strftime('%Y-%m-%dT%H:%M:%S.%sZ')) }
                        }
                        f = gFile(temp)
                        f.localPath = row[1]
                    else:
                        # build a temp object struct
                        temp = {
                            "id": str(row[0]),
                            "name": os.path.basename(row[1]),
                            "mimeType": row[3]
                            #"properties": { 'modifiedTime': str(datetime.datetime.fromtimestamp(row[4]).strftime('%Y-%m-%dT%H:%M:%S.%sZ')) }
                        }
                        f = gFile(temp)
                        f.localPath = row[1]
                        f.md5 = row[2]
                    results.append(f)
                    totalFetched +=1
                except Exception as err:
                    logging.error("unable to fetch db object and construct the appropriate structure. %s" % str(err))


        except sqlite3.Error as e:
            logging.error("Unable to fetch records. %s" % str(e))
        except Exception as e:
            logging.error("Unable to fetch records. %s" % str(e))
        
        return totalFetched, results
    
    def fetch_gObjectSet(self, pageSize:int = 100, offset:int=0, searchField:str = None, searchCriteria:str=None):
        gObjects = []
        totalFetched = 0
        try:
            fetchObjects_sql = "SELECT id, name, mime_type, md5, local_path, properties FROM gObjects "
            if searchField is not None and searchCriteria is not None:
                fetchObjects_sql =  fetchObjects_sql + "WHERE " + searchField + " LIKE ? "
                sqlParams = (searchCriteria, pageSize, offset)
            else:
                sqlParams = (pageSize, offset)
            fetchObjects_sql =  fetchObjects_sql + " LIMIT ? OFFSET ?;"
            self.cursor.execute(fetchObjects_sql, sqlParams)
            rows = self.cursor.fetchall()

            for row in rows:
                mimeType = row[2]
                if "folder" in mimeType:
                    f = gFolder(json.loads(row[5]))
                else:
                    f = gFile(json.loads(row[5]))
                    f.md5 = row[3]
                f.id = row[0]
                f.name = row[1]
                f.mimeType = row[2]
                f.localPath = row[4]
                #f.properties = json.loads(row[5])

                gObjects.append(f)

                totalFetched += 1
                
        except sqlite3.Error as e:
            logging.error("Unable to fetch records. %s" % str(e))
        except Exception as e:
            logging.error("Unable to fetch records. %s" % str(e))

        return totalFetched, gObjects


    def fetch_deletedObjects(self, pageSize:int = 100, offset:int=0):
        gObjects = []
        totalFetched = 0
        try:
            # there are duplicate files in Google drive, some of which are deleted
            # we just need to be careful about removing local files if there are 
            # versions of the file that arent' deleted. 
            # this query should accomplish just that.
            fetchObjects_sql = 'SELECT id, name, mime_type, md5, local_path, properties \
                                    FROM gObjects WHERE \
                                        json_extract(properties, "$.trashed") = 1 \
                                    AND local_path NOT IN ( \
                                        SELECT local_path \
                                        FROM gObjects \
                                        GROUP BY md5, local_path \
                                        HAVING count(md5) > 1 and count(local_path) > 1 \
                                    ) LIMIT ? OFFSET ?; '

            sqlParams = (pageSize, offset)
            self.cursor.execute(fetchObjects_sql, sqlParams)
            rows = self.cursor.fetchall()

            for row in rows:
                mimeType = row[2]
                if "folder" in mimeType:
                    f = gFolder(json.loads(row[5]))
                else:
                    f = gFile(json.loads(row[5]))
                    f.md5 = row[3]
                f.id = row[0]
                f.name = row[1]
                f.mimeType = row[2]
                f.localPath = row[4]
                #f.properties = json.loads(row[5])

                gObjects.append(f)

                totalFetched += 1
                
        except sqlite3.Error as e:
            logging.error("Unable to fetch deleted records. %s" % str(e))
        except Exception as e:
            logging.error("Unable to fetch deleted records. %s" % str(e))

        return gObjects, totalFetched

    def insert_gObject(self, folder:gFolder = None, file:gFile = None):
        if folder is not None and file is not None:
            raise("invalid parameter set.  supply folder or file option, not both.")
        elif folder is not None:
            self.__insert_gFolder(folder)
        else:
            self.__insert_gFile(file)


    def __insert_gFolder(self, folder: gFolder):
        try:
            f = self.fetch_gObject(folder.id)
            if len(f) == 1:
                f[0].properties = folder.properties
                f[0].localPath = folder.localPath
                self.__update_gFolder(folder=f[0])
            elif len(f) > 1:
                raise("folder already exists and more than one record in the database.  resolve manually")
            else:
                # do we want to base64 the properties json blob?
                procInsertObject_sql = "INSERT INTO gObjects\
                                        (id, name, mime_type, local_path, properties) VALUES (?, ?, ?, ?, ?);"
                sqlParams = (folder.id, folder.name, folder.mimeType, folder.localPath, json.dumps(folder.properties))
                self.cursor.execute(procInsertObject_sql, sqlParams)
                self.conn.commit()
        except sqlite3.Error as e:

            logging.error("unable to insert folder %s into database. %s" % (folder.name, str(e)))
        except Exception as e:
            logging.error("unable to insert folder %s into database. %s" % (folder.name, str(e)))

    def __insert_gFile(self, file: gFile):
        try:
            f = self.fetch_gObject(file.id)
            if len(f) == 1:
                f[0].md5 = file.md5
                f[0].properties = file.properties
                f[0].localPath = file.localPath
                self.update_gObject(file=f[0])
            elif len(f) > 1:
                raise("file already exists and more than one record in the database.  resolve manually")
            else:
                procInsertObject_sql = "INSERT INTO gObjects\
                                        (id, name, mime_type, properties, md5, local_path) VALUES (?, ?, ?, ?, ?, ?);"
                sqlParams = (file.id, file.name, file.mimeType, json.dumps(file.properties), file.md5, file.localPath)
                self.cursor.execute(procInsertObject_sql, sqlParams)
                self.conn.commit()

            if 'parents' in file.properties.keys():
                self.insert_parents(file.id, file.properties['parents'])
            else:
                logging.warning("%s file id doesn't have any parents." % file.id)

        except sqlite3.Error as e:
            logging.error("unable to insert file %s into database. %s" % (file.name, str(e)))
        except Exception as e:
            logging.error("unable to insert file %s into database. %s" % (file.name, str(e)))

    def insert_parents(self, id:str, parents: List[str]):
        try:
            existing_parents = sorted(self.fetch_parents(id))
            parents = sorted(parents)
            if existing_parents == parents:
                return
            elif len(existing_parents) > 0:
                self.update_parents(id, parents)
            else:
                #do the insert
                for parent in parents:
                    procInsertRelationships_sql = "INSERT INTO relationships (parent_id, child_id) VALUES (?, ?);"
                    sqlParams = (parent, id)
                    self.cursor.execute(procInsertRelationships_sql, sqlParams)
                self.conn.commit()

        except sqlite3.Error as e:
            logging.error("Unable to insert parents for object id %s. %s" % (id, str(e)))
        except Exception as e:
            logging.error("Unable to insert parents for object id %s. %s" % (id, str(e))) 
                
    def insert_localFile(self, path:str, md5: str, mime_type:str, last_mod: float):
        try:
            insert_localFile_sql = "INSERT INTO local_files (path, md5, mime_type, last_mod) values (?, ?, ?, ?);"
            sqlParams = (path, md5, mime_type, last_mod)
            self.cursor.execute(insert_localFile_sql, sqlParams)
            self.conn.commit()

        except sqlite3.Error as e:
            logging.error("Unable to insert parents for object id %s. %s" % (id, str(e)))
        except Exception as e:
            logging.error("Unable to insert parents for object id %s. %s" % (id, str(e))) 
    
    def update_gObject(self, folder: gFolder = None, file: gFile = None):
        if folder is not None and file is not None:
            raise("invalid parameter set.  supply folder or file option, not both.")
        elif folder is not None:
            self.__update_gFolder(folder)
        else:
            self.__update_gFile(file)

    def delete_gObject(self, id:str):
        try:
            deleteObject_sql = "DELETE FROM gObjects WHERE id = ?;"
            sqlParams = (id,)
    
            self.cursor.execute(deleteObject_sql, sqlParams)
            self.conn.commit()

        except sqlite3.Error as e:
            logging.error("Unable to delete object id %s. %s" % (id, str(e)))
        except Exception as e:
            logging.error("Unable to delete object id %s. %s" % (id, str(e)))


    def __update_gFolder(self, folder: gFolder):
        try:
            updateObject_sql = "UPDATE gObjects SET name = ?, properties = ?, local_path = ? WHERE id = ?;"
            sqlParams = (folder.name, json.dumps(folder.properties), folder.localPath, folder.id)
    
            self.cursor.execute(updateObject_sql, sqlParams)
            self.conn.commit()

            if 'parents' in folder.properties.keys():
                self.update_parents(folder.id, folder.properties['parents'])

        except sqlite3.Error as e:
            logging.error("Unable to update folder object id %s. %s" % (id, str(e)))
        except Exception as e:
            logging.error("Unable to update folder object id %s. %s" % (id, str(e)))

    def __update_gFile(self, file: gFile):
        try:
            updateObject_sql = "UPDATE gObjects SET name = ?, properties = ?, md5 = ?, local_path = ? WHERE id = ?;"
            sqlParams = (file.name, json.dumps(file.properties), file.md5, file.localPath, file.id)
    
            self.cursor.execute(updateObject_sql, sqlParams)
            self.conn.commit()
            

            if 'parents' in file.properties.keys():
                self.update_parents(file.id, file.properties['parents'])
            else:
                logging.warning("file id %s doesn't have any parents." % file.id)
            
        except sqlite3.Error as e:
            logging.error("Unable to fetch object id %s. %s" % (id, str(e)))
        except Exception as e:
            logging.error("Unable to fetch object id %s. %s" % (id, str(e)))

    def update_parents(self, id:str, parents: List[str]):
        try:
            self.lock.acquire(True)
            deleteParents_sql = "DELETE FROM relationships WHERE child_id = ?;"
            sqlParams = (id, )
            self.cursor.execute(deleteParents_sql, sqlParams)
            self.conn.commit()
            
            self.insert_parents(id, parents)
        
        except sqlite3.Error as e:
            logging.error("Unable to update parents for object id %s. %s" % (id, str(e)))
        except Exception as e:
            logging.error("Unable to update parents for object id %s. %s" % (id, str(e))) 
        finally:
            self.lock.release()

    def identify_local_deleted(self):
        try:
            # start a transaction for atomicity
            self.cursor.execute("BEGIN")

            # empty the table with deleted gObjects id
            update_sql = "DELETE FROM local_deleted;"
            self.cursor.execute(update_sql)

            # insert the deleted files ids into the temp table
            update_sql = "INSERT INTO local_deleted (deleted_id)\
                            SELECT gObjects.id FROM gObjects \
                            LEFT JOIN local_files ON gObjects.local_path = local_files.path \
                            WHERE local_files.path IS null;"
            self.cursor.execute(update_sql)

            # set the files trashed attribute to true
            update_sql = "UPDATE gObjects \
                            SET properties = json_patch(properties, '{" + '"' + 'trashed"' + ":true}') \
                            WHERE id IN ( \
                            SELECT deleted_id FROM local_deleted);"
            self.cursor.execute(update_sql)

            # increment the version of the file
            update_sql = "UPDATE gObjects \
                            SET properties = json_patch(properties, \
                                '{" + '"version"' + ":' || (json_extract(properties, '$.version')+1) || '}') \
                            WHERE gObjects.id IN ( \
                                SELECT deleted_id FROM local_deleted) \
                            AND json_extract(properties, '$.version') > 0;"
            self.cursor.execute(update_sql)
            # commit the transaction if successful
            self.conn.commit()          

        except sqlite3.Error as e:
            logging.error("Unable to update metadata for locally deleted files. %s" % str(e))
            self.cursor.execute("ROLLBACK;")
        except Exception as e:
            logging.error("Unable to update metadata for locally deleted files. %s" % str(e))
            self.cursor.execute("ROLLBACK;") 

    def get_files_deleted_from_disk(self, pageSize:int = 100, offset:int = 0):
        deleted_objects = []
        totalFetched = 0
        try:
            fetch_sql = "SELECT gObjects.id, name, mime_type, md5, local_path, properties FROM gObjects \
                            INNER JOIN local_deleted ON gObjects.id = local_deleted.deleted_id LIMIT ? OFFSET ?;"

            sqlParams = (pageSize, offset)
            self.cursor.execute(fetch_sql, sqlParams)
            rows = self.cursor.fetchall()
            for row in rows:
                try:
                    if row[2] == cfg.TYPE_GOOGLE_FOLDER:
                        f = gFolder(json.loads(row[5]))
                        f.localPath = row[4]
                    else:
                        f = gFile(json.loads(row[5]))
                        f.localPath = row[4]
                        f.md5 = row[3]
                    deleted_objects.append(f)
                    totalFetched +=1
                except Exception as err:
                    logging.error("unable to fetch db objects of deleted files. %s" % str(err))
        
        except sqlite3.Error as e:
            logging.error("Error fetching deleted files. %s" % str(e))
        except Exception as e:
            logging.error("Error fetching deleted files. %s" % str(e))

        return totalFetched, deleted_objects


    def delete_files_not_on_disk(self):
        try:

            delete_sql = 'DELETE FROM gObjects \
                            WHERE id IN (\
                            SELECT gObjects.id from gObjects\
                            LEFT JOIN local_files \
                            ON gObjects.md5 = local_files.md5 AND \
                            gObjects.local_path = local_files.path \
                            WHERE local_files.md5 IS NULL \
                            AND local_files.mime_type = "file" \
                            AND gObjects.mime_type NOT LIKE "%folder%" \
                            AND json_extract(properties, "$.trashed") = 0);'
            self.cursor.execute(delete_sql)
            self.conn.commit()

            delete_sql = 'DELETE from gObjects \
                            WHERE local_path NOT IN ( \
                            SELECT path FROM local_files) \
                            AND gObjects.local_path IS NOT null \
                            AND gObjects.mime_type NOT LIKE "%folder%" \
                            AND json_extract(gObjects.properties, "$.trashed") = 0;'
            self.cursor.execute(delete_sql)
            self.conn.commit()

            delete_sql = 'DELETE FROM gObjects \
                            WHERE id IN (\
                            SELECT gObjects.id from gObjects\
                            LEFT JOIN local_files \
                            ON gObjects.local_path = local_files.path \
                            WHERE local_files.path IS NULL \
                            AND local_files.mime_type = "directory" \
                            AND gObjects.mime_type LIKE "%folder%" \
                            AND json_extract(properties, "$.trashed") = 0);'
            self.cursor.execute(delete_sql)
            self.conn.commit()

        except sqlite3.Error as e:
            logging.error("Error deleting files not on disk. %s" % (id, str(e)))
        except Exception as e:
            logging.error("Error deleting files not on disk. %s" % (id, str(e)))


    def open(self, dbPath: str):
        try:
            self.conn = sqlite3.connect(dbPath, check_same_thread=False)
            self.cursor = self.conn.cursor()
        except sqlite3.Error as error:
            logging.error("error closing database. %s" % str(e))
        except Exception as e:
            logging.error("error closing database. %s" % str(e))
    
    def close(self):
        try:
            self.conn.close()
            self.cursor = None
            self.conn = None
        except sqlite3.Error as error:
            logging.error("error closing database. %s" % str(e))
        except Exception as e:
            logging.error("error closing database. %s" % str(e))
    
        