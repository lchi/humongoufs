# st_nlink will report number of directories underneath

from fuse import FUSE, FuseOSError
from pymongo import Connection
from bson.objectid import ObjectId
from stat import S_IFDIR, S_IFREG

import time
import bson
import errno
import json
import sys

class Mongo:
    def __init__(self, conn, validate=True):
        self.conn = conn
        if validate and  not self._isValid():
            raise FuseOSError(errno.ENOENT)

    def _isValid(self):
        return not self.conn is None

    def getattr(self):
        mc_time = time.mktime(self.conn['admin'].command('serverStatus')
                     ['backgroundFlushing']['last_finished'].timetuple())
        st_size = sum([self.conn[db].command('dbstats')['fileSize'] for db in 
                      [str(dbName) for dbName in self.conn.database_names()]])
        return dict(
            st_mode= (S_IFDIR | 0777),
            st_nlink=len(self.conn.database_names()),
            st_size=0,#st_size,
            st_ctime=mc_time,
            st_mtime=mc_time,
            st_atime=time.time())
            

    def readdir(self):
        return ['.', '..'] + [str(r) for r in self.conn.database_names()]

class Database:
    def __init__(self, conn, db, validate=True):
        self.conn = conn
        self.db = db
        if validate and not self._isValid():
            raise FuseOSError(errno.ENOENT)

    def _isValid(self):
        return self.db in self.conn.database_names()
    
    def getattr(self):
        mc_time = time.mktime(self.conn['admin'].command('serverStatus')
                              ['backgroundFlushing']['last_finished'].timetuple())
        st_size = self.conn[self.db].command('dbstats')['fileSize'] 

        return {
            'st_mode' : (S_IFDIR | 0777),
            'st_nlink' : len(self.conn.database_names()),
            'st_size' : st_size,
            'st_ctime' : mc_time,
            'st_mtime' : mc_time,
            'st_atime' : time.time()
            }

    def mkdir(self):
        self.conn[self.db].create_collection('tmp')
        self.conn[self.db].drop_collection('tmp')

    def readdir(self):
        return ['.', '..'] + [str(r) for r in self.conn[self.db].collection_names()]
    
    def rmdir(self):
        self.conn.drop_database(self.db)

class Collection:
    def __init__(self, conn, db, col, validate=True):
        self.conn = conn
        self.db = db
        self.col = col
        if validate and not self._isValid():
            raise FuseOSError(errno.ENOENT)
        
    def _isValid(self):
        return self.col in self.conn[self.db].collection_names()

    def getattr(self):
        mc_time = time.mktime(self.conn['admin'].command('serverStatus')
                              ['backgroundFlushing']['last_finished'].timetuple())
        st_size = self.conn[self.db].command('collStats', self.col)['storageSize']
        return {
            'st_mode' : (S_IFDIR | 0777),
            'st_nlink' : 1,
            'st_size' : st_size,
            'st_ctime' : 0,
            'st_mtime' : 0,
            'st_atime' : time.time()
            }
    
    def mkdir(self):
        self.conn[self.db].create_collection(self.col)

    def readdir(self):
        return ['.', '..'] + [str(r['_id']) for r in (self.conn[self.db])[self.col].find()]
    
    def rmdir(self):
        self.conn[self.db].drop_collection(self.col)

class Document:
    def __init__(self, conn, db, col, doc, validate=True):
        self.conn = conn
        self.db = db
        self.col = col
        self.doc = doc
        if validate and not self._isValid():
            raise FuseOSError(errno.ENOENT)
    
    def _isValid(self):
        try:
            return not ((self.conn[self.db])[self.col].find_one(
                    ObjectId(self.doc)) is None)
        except bson.errors.InvalidId, e:
            raise FuseOSError(errno.ENOENT)

    def getattr(self):
        obj = self.conn[self.db][self.col].find_one(ObjectId(self.doc))
        obj['_id'] = self.doc
        return {
            'st_mode' : (S_IFREG | 0777),
            'st_nlink' : 1,
            'st_size' : len(json.dumps(obj, indent=4)),
            'st_ctime' : 0,
            'st_mtime' : 0,
            'st_atime' : time.time()
            }

    def read(self):
        obj = self.conn[self.db][self.col].find_one(ObjectId(self.doc))
        obj['_id'] = self.doc
        return json.dumps(obj, indent=4)

    def readdir(self):
        raise FuseOSError(errno.ENOTDIR)

def parsePath(self, path):
    [s for s in path.split('/') if s]

