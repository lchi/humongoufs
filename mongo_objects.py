from fuse import FUSE, FuseOSError
from pymongo import Connection
import bson
import errno

class Mongo:
    def __init__(self, conn):
        self.conn = conn
        if not self._isValid():
            raise FuseOSError(errno.ENOENT)

    def _isValid(self):
        return not self.conn is None

    def readdir(self):
        return ['.', '..'] + [str(r) for r in self.conn.database_names()]

class Database:
    def __init__(self, conn, db):
        self.conn = conn
        self.db = db
        if not self._isValid():
            raise FuseOSError(errno.ENOENT)

    def _isValid(self):
        return self.db in self.conn.database_names()

    def readdir(self):
        return ['.', '..'] + [str(r) for r in self.conn[self.db].collection_names()]

class Collection:
    def __init__(self, conn, db, col):
        self.conn = conn
        self.db = db
        self.col = col
        if not self._isValid():
            raise FuseOSError(errno.ENOENT)
        
    def _isValid(self):
        return self.col in self.conn[self.db].collection_names()
    
    def readdir(self):
        return ['.', '..'] + [str(r['_id']) for r in (self.conn[db])[self.col].find()]

class Document:
    def __init__(self, conn, db, col, doc):
        self.conn = conn
        self.db = db
        self.col = col
        self.doc = doc
        if not self._isValid():
            raise FuseOSError(errno.ENOENT)
    
    def _isValid(self):
        try:
            return not ((self.conn[self.db])[self.col].find_one(
                    ObjectId(str(self.doc))) is None)
        except bson.errors.InvalidId, e:
            raise FuseOSError(errno.ENOENT)
    
    def readdir(self):
        raise FuseOSError(errno.ENOTDIR)

def parsePath(self, path):
    [s for s in path.split('/') if s]

def documentExists(self, db, col, doc):
    try:
        return not ((self.conn[db])[col].find_one(ObjectId(str(doc))) is None)
    except bson.errors.InvalidId, e:
        raise FuseOSError(errno.ENOENT)

def collectionExists(self, db, col):
    return col in self.conn[db].collection_names()
    
def databaseExists(self, db):
    return db in self.conn.database_names()

def validFilePath(self, pathList):
    if len(pathList) != 3:
        return False
    return (self.databaseExists(pathList[0]) and 
            self.collectionExists(pathList[0], pathList[1]) and
            self.documentExists(pathList[0], pathList[1], pathList[2]))

def validDirPath(self, pathList):
    result = True
    if not pathList:
        return True
    if len(pathList) > 2:
        return False
    if len(pathList) >= 1:
        result = result and self.databaseExists(pathList[0])
        if len(pathList) >= 2:
            result = result and self.collectionExists(pathList[0], pathList[1])
        return result