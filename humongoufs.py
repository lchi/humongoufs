#!/usr/bin/env python

import errno
from collections import defaultdict
from stat import S_IFDIR, S_IFLNK, S_IFREG
from sys import argv, exit
from time import time

from fuse import FUSE, FuseOSError, Operations, LoggingMixIn
from pymongo import Connection
from bson.objectid import ObjectId
from bson.errors import InvalidId

import mongo_objects

class Humongoufs(LoggingMixIn, Operations):
    """Example memory filesystem. Supports only one level of files."""
    
    def __init__(self, host, port):
        self.conn = Connection(host,port)

    def chmod(self, path, mode):
        raise FuseOSError(errno.EPERM)

    def chown(self, path, uid, gid):
        raise FuseOSError(errno.EPERM)
    
    def create(self, path, mode):
        self.files[path] = dict(st_mode=(S_IFREG | mode), st_nlink=1,
            st_size=0, st_ctime=time(), st_mtime=time(), st_atime=time())
        self.fd += 1
        return self.fd

    def destroy(self, path):
        self.conn.disconnect()
    
    def getattr(self, path, fh=None):
        obj = self.getObjectFromPath(path)
        return obj.getattr()

#    def getxattr(self, path, name, position=0):
#        attrs = self.files[path].get('attrs', {})
#        try:
#            return attrs[name]
#        except KeyError:
#            return ''       # Should return ENOATTR
    
#    def listxattr(self, path):
#        attrs = self.files[path].get('attrs', {})
#        return attrs.keys()
    
    def mkdir(self, path, mode):
        obj = self.makeNewObjectFromPath(path)
        if isinstance(obj, mongo_objects.Database) or isinstance(obj, mongo_objects.Collection):
            return obj.mkdir()
        else:
            raise FuseOSError(errno.EPERM)

#    def open(self, path, flags):
#        self.fd += 1
#        return self.fd
    
    def read(self, path, size, offset, fh):
        print 'size:', size, 'offset:', offset
        obj = self.getObjectFromPath(path)
        if isinstance(obj, mongo_objects.Document):
            return obj.read()
        else:
            raise FuseOSError(errno.EPERM)
            
    def readdir(self, path, fh):
        obj = self.getObjectFromPath(path)
        return obj.readdir()
            
#    def readlink(self, path):
#        return self.data[path]
    
#    def removexattr(self, path, name):
#        attrs = self.files[path].get('attrs', {})
#        try:
#            del attrs[name]
#        except KeyError:
#            pass        # Should return ENOATTR
    
    def rename(self, old, new):
        self.files[new] = self.files.pop(old)
    
    def rmdir(self, path):
        obj = self.makeNewObjectFromPath(path)
        if isinstance(obj, mongo_objects.Database) or isinstance(obj, mongo_objects.Collection):
            return obj.rmdir()
        else:
            raise FuseOSError(errno.EPERM)
    
#    def setxattr(self, path, name, value, options, position=0):
    
    def statfs(self, path):
        return dict(f_bsize=512, f_blocks=4096, f_bavail=2048)
    
    def symlink(self, target, source):
        self.files[target] = dict(st_mode=(S_IFLNK | 0777), st_nlink=1,
            st_size=len(source))
        self.data[target] = source
    
    def truncate(self, path, length, fh=None):
        self.data[path] = self.data[path][:length]
        self.files[path]['st_size'] = length
    
    def unlink(self, path):
        raise FuseOSError(errno.EPERM)
    
    def utimens(self, path, times=None):
        now = time()
        atime, mtime = times if times else (now, now)
        self.files[path]['st_atime'] = atime
        self.files[path]['st_mtime'] = mtime
    
    def write(self, path, data, offset, fh):
        self.data[path] = self.data[path][:offset] + data
        self.files[path]['st_size'] = len(self.data[path])
        return len(data)

    '''Helper functions'''
    def parsePath(self, path):
        return [s for s in path.split('/') if s]

    def getObjectFromPath(self, path):
        pp = self.parsePath(path)
        if not pp:
            return mongo_objects.Mongo(self.conn)
        elif len(pp) == 1:
            return mongo_objects.Database(self.conn, pp[0])
        elif len(pp) == 2:
            return mongo_objects.Collection(self.conn, pp[0], pp[1])
        elif len(pp) == 3:
            return mongo_objects.Document(self.conn, pp[0], pp[1], pp[2])
        else:
            raise FuseOSError(errno.ENOENT)
        
    def makeNewObjectFromPath(self, path):
        pp = self.parsePath(path)
        if len(pp) == 1:
            return mongo_objects.Database(self.conn, pp[0], False)
        elif len(pp) == 2:
            return mongo_objects.Collection(self.conn, pp[0], pp[1], False)
        elif len(pp) == 3:
            return mongo.objects.Document(self.conn, pp[0], pp[1], pp[2], False)
        else:
            raise FuseOSError(errno.EPERM)

                           
def findOpt(option, args):
    if option in args and args.index(option) < len(args) - 1:
        return args.index(option) + 1
    return -1

if __name__ == "__main__":
    if len(argv) < 2:
        print 'usage: %s <options> <mountpoint>' % argv[0]
        exit(1)

    host = 'localhost'
    port = 27017

    idx = findOpt('-h', argv)
    if idx > 0: # host specified
        host = argv[idx]
        idx = -1
    idx = findOpt('-p', argv)
    if idx > 0: # port specified
        port = argv[idx]
        idx = -1
    
    fuse = FUSE(Humongoufs(host, port), argv[1], foreground=True)
