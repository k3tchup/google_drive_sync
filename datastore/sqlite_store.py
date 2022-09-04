import sqlite3
from sqlite3 import SQLITE_PRAGMA, Error
import logging
from typing import List
import json
import sys
import os


current = os.path.dirname(os.path.realpath(__file__))
parent = os.path.dirname(current)
sys.path.append(parent)
from gDrive_data_structures.data_types import *

class sqlite_store:
    def __init__(self):
        self.conn = None
        self.cursor = None
        return

    def __create_schema(self):
        logging.debug("creating database schema.")
        try:
            gObjectTable_sql = "CREATE TABLE IF NOT EXISTS gObjects (\
                                id nvarchar(100) PRIMARY KEY,\
                                name text NOT NULL,\
                                mime_type text NOT NULL,\
                                properties text NOT NULL\
                            );"
            parentChildrenTable_sql = "CREATE TABLE IF NOT EXISTS relationships (\
                                id integer PRIMARY KEY,\
                                parent_id nvarchar(100) NOT NULL,\
                                child_id nvarchar(100) NOT NULL,\
                                FOREIGN KEY (parent_id) REFERENCES gObjects (id),\
                                FOREIGN KEY (child_id) REFERENCES gObjects (id)\
                            );"

            procInsertObject_sql = "INSERT INTO gObjects\
                                    (id, Name, Joining_date, salary) VALUES (%s,%s,%s,%s)"
            
            self.cursor = self.conn.cursor()
            self.cursor.execute(gObjectTable_sql)
            self.cursor.execute(parentChildrenTable_sql)
        except sqlite3.Error as error:
            logging.error("error creating database schema %s." % str(err))
        except Exception as err:
            logging.error("error creating database schema %s." % str(err))

    def create_db(self, dbPath: str):
        logging.debug("creating database %s" % dbPath)
        try:
            self.conn = sqlite3.connect(dbPath, check_same_thread=False)
            self.__create_schema()
        except sqlite3.Error as error:
            logging.error("unable to create databaase %s. %s" % (dbPath, str(e)))
        except Exception as e:
            logging.error("unable to create databaase %s. %s" % (dbPath, str(e)))

    def fetch_gObject(self, id: str):
        logging.debug("fetching database object with id %s" % id)
        objects = []
        try:
            fetchObject_sql = "SELECT * FROM gObjects WHERE id = ?;"
            sqlParams = (id, )
            self.cursor.execute(fetchObject_sql, sqlParams)
            rows = self.cursor.fetchall()
        
            for row in rows:
                if row[2] == 'application/vnd.google-apps.folder':
                    folder = gFolder(json.loads(row[3]))
                    objects.append(folder)
                else:
                    file = gFile(json.loads(row[3]))
                    objects.append(file)

        except sqlite3.Error as e:
            logging.error("Unable to fetch object id %s. %s" % (id, str(e)))
        except Exception as e:
            logging.error("Unable to fetch object id %s. %s" % (id, str(e)))

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
                self.__update_gFolder(f[0])
            elif len(f) > 1:
                raise("folder already exists and more than one record in the database.  resolve manually")
            else:
                # do we want to base64 the properties json blob?
                procInsertObject_sql = "INSERT INTO gObjects\
                                        (id, name, mime_type, properties) VALUES (?, ?, ?, ?);"
                sqlParams = (folder.id, folder.name, folder.mimeType, json.dumps(folder.properties))
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
                self.update_gObject(f[0])
            elif len(f) > 1:
                raise("file already exists and more than one record in the database.  resolve manually")
            else:
                procInsertObject_sql = "INSERT INTO gObjects\
                                        (id, name, mime_type, properties) VALUES (?, ?, ?, ?);"
                sqlParams = (file.id, file.name, file.mimeType, json.dumps(file.properties))
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
                
    def update_gObject(self, folder: gFolder = None, file: gFile = None):
        if folder is not None and file is not None:
            raise("invalid parameter set.  supply folder or file option, not both.")
        elif folder is not None:
            self.__update_gFolder(folder)
        else:
            self.__update_gFile(file)


    def __update_gFolder(self, folder: gFolder):
        try:
            updateObject_sql = "UPDATE gObjects SET name = ?, properties = ? WHERE id = ?;"
            sqlParams = (folder.name, json.dumps(folder.properties), folder.id)
    
            self.cursor.execute(updateObject_sql, sqlParams)
            self.conn.commit()

            if 'parents' in folder.properties.keys():
                self.update_parents(folder.id, folder.properties['parents'])

        except sqlite3.Error as e:
            logging.error("Unable to update folder object id %s. %s" % (id, str(e)))
        except Exception as e:
            logging.error("Unable to update folder object id %s. %s" % (id, str(e)))

    def __update_gFile(self, file: gFolder):
        try:
            updateObject_sql = "UPDATE gObjects SET name = ?, properties = ? WHERE id = ?;"
            sqlParams = (file.name, json.dumps(file.properties), file.id)
    
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
            deleteParents_sql = "DELETE FROM relationships WHERE child_id = ?;"
            sqlParams = (id, )
            self.cursor.execute(deleteParents_sql, sqlParams)
            self.conn.commit()
            
            self.insert_parents(id, parents)
        
        except sqlite3.Error as e:
            logging.error("Unable to update parents for object id %s. %s" % (id, str(e)))
        except Exception as e:
            logging.error("Unable to update parents for object id %s. %s" % (id, str(e))) 


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
    
        