# -*- coding:utf-8 -*-

from abc import ABCMeta, abstractmethod
from datetime import datetime

import ftplib
import ftputil
import ftputil_custom
import os
import os.path
import posixpath
import re
import shutil

def getFileSystem(path):
    fileSystems = [
        WindowsFileSystem(),
        UnixFileSystem(),
        FTPFileSystem()
    ]

    for fs in fileSystems:
        if fs.isSupportedPath(path):
            return fs.init(path)

    return None

class FileSystem(metaclass=ABCMeta):
    def __init__(self):
        self.supportedPathPatterns = list()

    def isSupportedPath(self, path):
        """Retourne vrai si le format de chemin est pris en charge."""
        if self.foundPathPattern(path):
            return True

        return False

    def foundPathPattern(self, path):
        """Cherche un pattern compatible avec le chemin passé en paramètre.

        Si un pattern est trouvé, un tuple est renvoyé contenant le pattern
        et l'objet ReMatch correspondant.
        """
        for pattern in self.supportedPathPatterns:
            results = re.match(pattern, path, re.IGNORECASE)

            if results:
                return pattern, results

        return None

    @abstractmethod
    def mkdir(self, path): pass

    @abstractmethod
    def makedirs(self, path): pass

    @abstractmethod
    def rmdir(self, path): pass

    @abstractmethod
    def rmtree(self, path): pass

    @abstractmethod
    def open(self, path, mode): pass

    @abstractmethod
    def read(self, filename): pass

    @abstractmethod
    def write(self, filename, content=None, fd_content=None): pass

    @abstractmethod
    def delete(self, filename): pass

    @abstractmethod
    def stat(self, filename): pass

    @abstractmethod
    def utime(self, path, times): pass

    @abstractmethod
    def walk(self, path): pass

    @abstractmethod
    def init(self, path): pass

class WindowsFileSystem(FileSystem):
    def __init__(self):
        super().__init__()

        self.supportedPathPatterns = [
            "^[a-z]:[\\\/].*"
        ]

    def mkdir(self, path): pass

    def makedirs(self, path):
        path = posixpath.join(self.basepath, path)
        os.makedirs(path, exist_ok=True)

    def rmdir(self, path): pass

    def rmtree(self, path): 
        path = posixpath.join(self.basepath, path)
        shutil.rmtree(path, ignore_errors=True)

    def open(self, path, mode):
        path = posixpath.join(self.basepath, path)

        return open(path, mode)

    def read(self, filename): 
        filename = posixpath.join(self.basepath, filename)

        fd = open(filename, "rb")
        content = fd.read()
        fd.close()
        
        return content

    def write(self, filename, content=None, fd_content=None): 
        if fd_content:
            content = fd_content.read()

        filename = posixpath.join(self.basepath, filename)
        
        fd = open(filename, "wb")
        fd.write(content)
        fd.close()

    def delete(self, filename): 
        filename = posixpath.join(self.basepath, filename)
        os.unlink(filename)

    def stat(self, path): 
        stat = os.lstat(path)

        # Modification de st_*time pour avoir des timestamps UTC
        _stat = list(stat)
        _stat[7] = datetime.utcfromtimestamp(_stat[7]).timestamp()
        _stat[8] = datetime.utcfromtimestamp(_stat[8]).timestamp()
        _stat[9] = datetime.utcfromtimestamp(_stat[9]).timestamp()

        return os.stat_result(_stat)

    def utime(self, path, times):
        path = posixpath.join(self.basepath, path)
        os.utime(path, times)

    def walk(self, path):
        return os.walk(path)

    def init(self, path): 
        """Initialise l'accès au système de fichiers."""
        self.basepath = path

        return self

class UnixFileSystem(FileSystem):
    def __init__(self):
        super().__init__()

        self.supportedPathPatterns = [
            "/.*"
        ]

    def mkdir(self, path): pass

    def makedirs(self, path): 
        path = posixpath.join(self.basepath, path)
        os.makedirs(path, exist_ok=True)

    def rmdir(self, path): pass

    def rmtree(self, path): pass

    def open(self, path, mode):
        path = posixpath.join(self.basepath, path)

        return open(path, mode)

    def read(self, filename): 
        filename = posixpath.join(self.basepath, filename)

        fd = open(filename, "rb")
        content = fd.read()
        fd.close()
        
        return content

    def write(self, filename, content=None, fd_content=None):
        if fd_content:
            content = fd_content.read()

        filename = posixpath.join(self.basepath, filename)
        
        fd = open(filename, "wb")
        fd.write(content)
        fd.close()

    def delete(self, filename): 
        filename = posixpath.join(self.basepath, filename)
        #os.unlink(filename)

    def stat(self, path):
        stat = os.lstat(path)

        # Modification de st_*time pour avoir des timestamps UTC
        _stat = list(stat)
        _stat[7] = datetime.utcfromtimestamp(_stat[7]).timestamp()
        _stat[8] = datetime.utcfromtimestamp(_stat[8]).timestamp()
        _stat[9] = datetime.utcfromtimestamp(_stat[9]).timestamp()
        
        return os.stat_result(_stat)

    def utime(self, path, times):
        path = posixpath.join(self.basepath, path)
        os.utime(path, times)

    def walk(self, path):
        return os.walk(path)

    def init(self, path): 
        """Initialise l'accès au système de fichiers."""
        self.basepath = path

        return self

class FTPFileSystem(FileSystem):
    def __init__(self):
        super().__init__()

        self.supportedPathPatterns = [
            "^ftp://((.+):(.+)@)?([^:/]+)(:([0-9]{1,5}))?(/.+)*$"
        ]

    def mkdir(self, path): pass

    def makedirs(self, path): 
        path = posixpath.join(self.basepath, path)
        self.ftp.makedirs(path)

    def rmdir(self, path): pass

    def rmtree(self, path):
        path = posixpath.join(self.basepath, path)
        self.ftp.rmtree(path, ignore_errors=True)

    def open(self, path, mode): 
        path = posixpath.join(self.basepath, path)

        return self.ftp.open(path, "rb")

    def read(self, filename): pass

    def write(self, filename, content=None, fd_content=None):
        filename = posixpath.join(self.basepath, filename)
        
        fd = self.ftp.open(filename, "wb")
        self.ftp.copyfileobj(fd_content, fd)
        fd.close()

    def delete(self, filename): 
        filename = posixpath.join(self.basepath, filename)
        self.ftp.unlink(filename)

    def utime(self, path, times):
        path = posixpath.join(self.basepath, path)

        mtime = datetime.utcfromtimestamp(times[1]).strftime("%Y%m%d%H%M%S")
        
        self.ftp._session.sendcmd("MFMT {mtime} {path}".format(
            mtime=mtime,
            path=path))

    def stat(self, path):
        return self.ftp.lstat(path)

    def walk(self, path): 
        return self.ftp.walk(path)

    def init(self, path):
        """Initialise l'accès au système de fichiers."""
        self.basepath = "/"

        pattern, results = self.foundPathPattern(path)

        user = "anonymous"
        password = ""
        port = 0

        if results.group(2):
            user = results.group(2)
            password = results.group(3)

        server = results.group(4)

        if results.group(6):
            port = int(results.group(6))

        if results.group(7):
            self.basepath = results.group(7)

        self.ftp = ftputil.FTPHost(
            host=server, 
            port=port, 
            user=user, 
            password=password, 
            session_factory=ftputil_custom.FTPSession)
        self.ftp._stat = ftputil_custom._StatMLSD(self.ftp)
        self.ftp.chdir(self.basepath)

        return self
