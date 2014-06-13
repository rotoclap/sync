# -*- coding:utf-8 -*-

import datetime
import ftplib
import os.path
import posixpath
import pytz
import re
import time
from abc import ABCMeta, abstractmethod

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
    def create(self, filename): pass

    @abstractmethod
    def read(self, filename): pass

    @abstractmethod
    def write(self, filename, value): pass

    @abstractmethod
    def delete(self, filename): pass

    @abstractmethod
    def stat(self, filename): pass

    @abstractmethod
    def walk(self, path): pass

    @abstractmethod
    def init(self, path): pass

class WindowsFileSystem(FileSystem):
    def __init__(self):
        super(WindowsFileSystem, self).__init__()

        self.supportedPathPatterns = [
            "^[a-z]:[\\\/].*"
        ]

    def create(self, filename): pass

    def read(self, filename): pass

    def write(self, filename, value): pass

    def delete(self, filename): pass

    def stat(self, path): pass

    def walk(self, path): pass

    def init(self, path): 
        """Initialise l'accès au système de fichiers."""
        self.basepath = path

        return self

class UnixFileSystem(FileSystem):
    def __init__(self):
        super(UnixFileSystem, self).__init__()

        self.supportedPathPatterns = [
            "/.*"
        ]

    def create(self, filename): pass

    def read(self, filename): pass

    def write(self, filename, value): pass

    def delete(self, filename): pass

    def stat(self, path): pass

    def walk(self, path): pass

    def init(self, path): 
        """Initialise l'accès au système de fichiers."""
        self.basepath = path

        return self

class FTPFileSystem(FileSystem):
    def __init__(self):
        super(FTPFileSystem, self).__init__()

        self.supportedPathPatterns = [
            "^ftp://((.+):(.+)@)?([^:]+)(:([0-9]{1,5}))?(/.+)*$"
        ]
        self.lastRefresh = 0
        self.files = None

    def create(self, filename): pass

    def read(self, filename): pass

    def write(self, filename, value): pass

    def delete(self, filename): pass

    def stat(self, path): pass

    def walk(self): 
        """Renvoie l'aborescence complète relatif au dossier.

        Equivalent du os.walk() mais pour une connexion FTP.
        Si l'arborescence a déjà été renvoyé il y a moins d'une minute,
        le serveur FTP n'est pas interrogé et la version en cache est
        renvoyée.
        """
        if time.time() - self.lastRefresh >= 60:
            self.files = self.__walk(self.basepath)
            self.lastRefresh = time.time()

        files = list()

        for root, _dirs, _files in self.files:
            files.append([root, list(_dirs.keys()), list(_files.keys())])

        files = [[posixpath.relpath(root, self.basepath), _dirs, _files] \
            for root, _dirs, _files in files]
        return files

    def __walk(self, root="/", files=None):
        """Fonction récursive d'exploration d'un dossier.

        Pour chaque dossier, un tuple est renvoyé contenant:
        - Le chemin du dossier courant
        - Un dictionnaire des sous-dossiers (avec comme clé le nom du dossier
          et en valeur sa date de modification "mdate" en UTC)
        - Un dictionnaire des fichiers (avec comme clé le nom du fichier et en
          valeur un dictionnaire de leurs attributs: size et mdate)
        """
        if files == None:
            files = list()

        entry = [root, dict(), dict()]

        for _file in self.ftp.mlsd(root):
            if _file[1]["type"] == "dir":
                entry[1][_file[0]] = \
                    pytz.utc.localize(datetime.datetime.strptime(
                        _file[1]["modify"], "%Y%m%d%H%M%S"))
                self.__walk(posixpath.join(root, _file[0]), files)
            else:
                entry[2][_file[0]] = {
                    "size": _file[1]["size"],
                    "mdate": pytz.utc.localize(datetime.datetime.strptime(
                        _file[1]["modify"], "%Y%m%d%H%M%S"))
                }

        files.append(entry) 

        return files

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

        self.ftp = ftplib.FTP()
        self.ftp.encoding="utf-8"
        self.ftp.connect(server, port)
        self.ftp.login(user, password)
        self.ftp.cwd(self.basepath)

        return self
