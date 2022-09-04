import sqlite3
from sqlite3 import Error
import logging
from typing import Optional
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
        try:
            self.conn = sqlite3.connect(dbPath)
            self.__create_schema()
        except sqlite3.Error as error:
            logging.error("unable to create databaase %s. %s" % (dbPath, str(e)))
        except Exception as e:
            logging.error("unable to create databaase %s. %s" % (dbPath, str(e)))

    def fetch_gObject(self, id: str):
        objects = []
        try:
            fetchObject_sql = "SELECT * FROM gObjects WHERE id = '" + id + "';"
            self.cursor.execute(fetchObject_sql)
            rows = self.cursor.fetchall()
        
            for row in rows:
                if row[2] == 'application/vnd.google-apps.folder':
                    folder = gFolder(json.loads(row[3]))
                    objects.append(folder)
                else:
                    file = gFile(json.loads(row[3]))
                    objects.append(folder)

        except sqlite3.Error as e:
            logging.error("Unable to fetch object id %s. %s" % (id, str(e)))
        except Exception as e:
            logging.error("Unable to fetch object id %s. %s" % (id, str(e)))

        return objects

    def fetch_parents(self, id: str):
        parents = []
        try:
            fetchObject_sql = "SELECT * FROM relationships WHERE child_id = '" + id + "';" 

            self.cursor.execute(fetchObject_sql)
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
                self.update_gObject(f[0])
            elif len(f) > 1:
                raise("folder already exists and more than one record in the database.  resolve manually")
            else:
                # do we want to base64 the properties json blob?
                procInsertObject_sql = "INSERT INTO gObjects\
                                        (id, name, mime_type, properties) VALUES ('" +\
                                            folder.id + "','"  +\
                                            folder.name + "','"  +\
                                            folder.mimeType + "','"  +\
                                            json.dumps(folder.properties) + "');"

                self.cursor.execute(procInsertObject_sql)
                self.conn.commit()
        except sqlite3.Error as e:

            logging.error("unable to insert folder %s into database. %s" % (folder.name, str(e)))
        except Exception as e:
            logging.error("unable to insert folder %s into database. %s" % (folder.name, str(e)))

    def __insert_gFile(self, file: gFile, i):
        try:
            procInsertObject_sql = "INSERT INTO gObjects\
                                      (id, name, mime_type, properties) VALUES ('" +\
                                        file.id + "','"  +\
                                        file.name + "','"  +\
                                        file.mimeType + "','"  +\
                                        json.dumps(file.properties) + "');"

            self.cursor.execute(procInsertObject_sql)
            self.conn.commit()

            for parent in file.properties['parents']:
                procInsertRelationships_sql = "INSERT INTO relationships \
                                            (parent_id, child_id) VALUES ('" +\
                                            parent + "', '" + \
                                            file.id + "');"
                self.cursor.execute(procInsertRelationships_sql)
                self.conn.commit()

        except sqlite3.Error as e:
            logging.error("unable to insert folder %s into database. %s" % (file.name, str(e)))
        except Exception as e:
            logging.error("unable to insert folder %s into database. %s" % (file.name, str(e)))


    def update_gObject(self, folder: gFolder):
        try:
            updateObject_sql = "UPDATE gObjects SET "\
                                    "name = '" + folder.name + "'," + \
                                    "properties = '" + json.dumps(folder.properties) + "' " + \
                                    "WHERE id = '" + folder.id + "';"
            
            
            self.cursor.execute(updateObject_sql)
            self.conn.commit()
        
        except sqlite3.Error as e:
            logging.error("Unable to fetch object id %s. %s" % (id, str(e)))
        except Exception as e:
            logging.error("Unable to fetch object id %s. %s" % (id, str(e)))


    def open(self, dbPath: str):
        try:
            self.conn = sqlite3.connect(dbPath)
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
    
        