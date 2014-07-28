# -*- coding:utf-8 -*-
from datetime import datetime

import argparse
import filesystem
import logging
import os.path
import posixpath
import pytz

class Sync:
    """Classe permettant de synchroniser deux répertoires"""

    def __init__(self, config, log=None):
        self.setDirLeft(config.dirLeft)
        self.setDirRight(config.dirRight)
        self.config = config

        if not log:
            log = logging.getLogger("null")
            log.addHandler(logging.NullHandler())

        self.log = log

        self.__syncInfosUpdated = False

    def setDirLeft(self, path):
        self.dirLeft = SyncDirectory(path)
        self.__syncInfosUpdated = False
        
        return self

    def setDirRight(self, path):
        self.dirRight = SyncDirectory(path)
        self.__syncInfosUpdated = False

        return self

    def sync(self):
        """Synchronization des fichiers."""

        if not self.__syncInfosUpdated:
            self.updateSyncInfos()

        if self.config.mirroring:
            self._buildFilesListsForMirror()
        else:
            self._buildFilesListsForSync()

        # Suppression des fichiers
        filesSide1, filesSide2 = self.filesToRemove.values()
        if (len(filesSide1) + len(filesSide2)) > 0 :
            self.log.info("Suppression des fichiers...")

        self._doRemoveFiles()

        # Suppression des dossiers        
        filesSide1, filesSide2 = self.dirsToRemove.values()
        if (len(filesSide1) + len(filesSide2)) > 0 :
            self.log.info("Suppression des dossiers...")
            
        self._doRemoveDirs()

        # Création des dossiers
        filesSide1, filesSide2 = self.dirsToCopy.values()
        if (len(filesSide1) + len(filesSide2)) > 0 :
            self.log.info("Création des dossiers...")
            
        self._doCopyDirs()

        # Copie des fichiers
        filesSide1, filesSide2 = self.filesToCopy.values()
        if (len(filesSide1) + len(filesSide2)) > 0 :
            self.log.info("Copie des fichiers...")
            
        self._doCopyFiles()

        return self

    def updateSyncInfos(self):
        """Mise à jour des infos de synchronisation."""
        self.dirsOnlyLeftSide = self.dirLeft.dirs.keys() - self.dirRight.dirs.keys()
        self.dirsOnlyRightSide = self.dirRight.dirs.keys() - self.dirLeft.dirs.keys()

        self.filesOnlyLeftSide = self.dirLeft.files.keys() - self.dirRight.files.keys()
        self.filesOnlyRightSide = self.dirRight.files.keys() - self.dirLeft.files.keys()

        self.filesMoreRecentLeftSide = self._updateMoreRecentFiles(self.dirLeft.files, self.dirRight.files)
        self.filesMoreRecentRightSide = self._updateMoreRecentFiles(self.dirRight.files, self.dirLeft.files)

        self.__syncInfosUpdated = True

    def _updateMoreRecentFiles(self, filesToCheck, filesReference):
        """Renvoie les fichiers plus récents par rapport aux fichiers de référence"""
        common_files = filesToCheck.keys() - (filesToCheck.keys() - filesReference.keys())

        files = set()

        for _file in common_files:
            if filesToCheck[_file]["mdate"] > filesReference[_file]["mdate"]:
                files.add(_file)

        return files

    def _buildFilesListsForSync(self):
        self.log.info("Mode synchronisation.")

        self.dirsToCopy = dict()
        self.dirsToCopy["left"] = self.dirsOnlyLeftSide
        self.dirsToCopy["right"] = self.dirsOnlyRightSide

        self.filesToCopy = dict()
        self.filesToCopy["left"] = self.filesOnlyLeftSide.union(self.filesMoreRecentLeftSide)
        self.filesToCopy["right"] = self.filesOnlyRightSide.union(self.filesMoreRecentRightSide)

        self.dirsToRemove = {
            "left" : set(),
            "right" : set()
        }

        self.filesToRemove = {
            "left" : set(),
            "right" : set()
        }

    def _buildFilesListsForMirror(self):
        self.log.info("Mode mirroir.")

        self.filesToRemove = dict()
        self.filesToRemove["left"] = set()
        self.filesToRemove["right"] = self.filesOnlyRightSide.union(self.filesMoreRecentRightSide)

        self.dirsToRemove = dict()
        self.dirsToRemove["left"] = set()
        self.dirsToRemove["right"] = self.dirsOnlyRightSide

        self.dirsToCopy = dict()
        self.dirsToCopy["left"] = self.dirsOnlyLeftSide
        self.dirsToCopy["right"] = set()

        self.filesToCopy = dict()
        self.filesToCopy["left"] = self.filesOnlyLeftSide.union(
            self.filesMoreRecentLeftSide, 
            self.filesMoreRecentRightSide)
        self.filesToCopy["right"] = set()

    def _doRemoveDirs(self):
        for side, paths in self.dirsToRemove.items():
            for path in paths:
                if side == "left":
                    self.log.debug("[G] {}...".format(path))
                    self.dirLeft.fs.rmtree(path)
                elif side == "right":
                    self.log.debug("[D] {}...".format(path))
                    self.dirRight.fs.rmtree(path)

    def _doRemoveFiles(self):
        for side, paths in self.filesToRemove.items():
            for path in paths:
                if side == "left":
                    self.log.debug("[G] {}...".format(path))
                    self.dirLeft.fs.delete(path)
                elif side == "right":
                    self.log.debug("[D] {}...".format(path))
                    self.dirRight.fs.delete(path)

    def _doCopyDirs(self):
        for side, paths in self.dirsToCopy.items():
            for path in paths:
                if side == "left":
                    self.log.debug("[G] {}...".format(path))
                    self.dirRight.fs.makedirs(path)
                elif side == "right":
                    self.log.debug("[D] {}...".format(path))
                    self.dirLeft.fs.makedirs(path)

    def _doCopyFiles(self):
        tz_paris = pytz.timezone("Europe/Paris")

        for side, paths in self.filesToCopy.items():
            for path in paths:
                if side == "left":
                    # Modification de la date de modification pour correspondre
                    # à celle du fichier source
                    abs_path = posixpath.join(self.dirLeft.fs.basepath, path)

                    utc_mtime = pytz.utc.localize(
                        datetime.fromtimestamp(self.dirLeft.fs.stat(abs_path).st_mtime))
                    local_mtime = utc_mtime.astimezone(tz_paris).timestamp()
                    
                    self.dirRight.fs.write(path, fd_content=self.dirLeft.fs.open(path, "rb"))
                    self.dirRight.fs.utime(path, (local_mtime, local_mtime))

                    self.log.debug("[G] {}".format(path))
                elif side == "right":
                    # Modification de la date de modification pour correspondre
                    # à celle du fichier source
                    abs_path = posixpath.join(self.dirRight.fs.basepath, path)

                    utc_mtime = pytz.utc.localize(
                        datetime.fromtimestamp(self.dirRight.fs.stat(abs_path).st_mtime))
                    local_mtime = utc_mtime.astimezone(tz_paris).timestamp()
                    
                    self.dirLeft.fs.write(path, fd_content=self.dirRight.fs.open(path, "rb"))
                    self.dirLeft.fs.utime(path, (local_mtime, local_mtime))

                    self.log.debug("[D] {}".format(path))

class SyncConfiguration:
    """Classe stockant les différents paramètres de synchronisation"""

    def __init__(self, parser=None):
        if parser:
            self.processArgs(parser)

    def __str__(self):
        infos =""

        if self.debug:
            infos = infos + "Debug activé.\n"

        if self.logActivated:
            infos = infos + "Fichier log : " + self.logpath + "\n"

        if self.mirroring:
            infos = infos + "Mode miroir activé.\n"

        infos = infos + "Dossier gauche : " + self.dirLeft + "\n"\
            + "Dossier droite : " + self.dirRight + "\n"

        if self.mirroring and self.preserveDirRight:
            infos = infos + \
                "Les fichiers n'existant que dans le dossier de droite " + \
                "ne seront pas supprimés.\n"

        if infos[-1] == "\n":
            infos = infos[:-1]
    
        return infos

    def processArgs(self, parser):
        """Interprète les arguments de la ligne de commande."""
        args = parser.parse_args()

        self.debug = args.debug
        self.logpath = os.path.abspath(args.logpath)
        self.mirroring = args.mirroring
        self.logActivated = args.log_activated
        self.preserveDirRight = args.preserve_dirright
        self.dirLeft = args.dirleft
        self.dirRight = args.dirright

        return self

class SyncDirectory:
    def __init__(self, basepath):
        self.fs = None
        self.basepath = basepath

    def __str__(self):
        return self.basepath

    def __getattr__(self, name):
        if name == "dirs":
            return self.__dirs()
        elif name == "files":
            return self.__files()
        elif name == "size":
            return self.__size()
        else:
            raise AttributeError()

    def __dirs(self):
        """Liste des dossiers.

        Le renvoi se fait sous la forme d'un dictionnaire dont la clé est le
        chemin relatif du dossier et la valeur un dictionnaire contenant les
        clés suivantes : 
        - "size" : taille du dossier en octet
        - "mdate": date de modification du dossier en timestamp UTC
        """
        if not self._dirs.keys():
            self.scan()

        return self._dirs

    def __files(self):
        """Liste des fichiers.

        Le renvoi se fait sous la forme d'un dictionnaire dont la clé est le
        chemin relatif du fichier et la valeur un dictionnaire contenant les
        clés suivantes : 
        - "size" : taille du fichier en octet
        - "mdate": date de modification du fichier en timestamp UTC
        """
        if not self._files.keys():
            self.scan()

        return self._files

    def __size(self):
        """Taille du répertoire (en octet)."""

        size = 0

        for stat in self.files.values():
            size = size + stat["size"]

        return size

    def attachFileSystem(self, path):
        self.fs = filesystem.getFileSystem(path)

    def scan(self):
        if not self.fs:
            self.attachFileSystem(self.basepath)
        
        self._dirs = dict()
        self._files = dict()

        for root, _dirs, _files in self.fs.walk(self.fs.basepath):
            for _dir in _dirs:
                path = os.path.join(root, _dir).replace("\\", "/")
                
                stat = self.fs.stat(path)

                path = posixpath.relpath(path, self.fs.basepath)

                self._dirs[path] = {
                    "size": stat.st_size,
                    "mdate": stat.st_mtime
                }

            for _file in _files:
                path = os.path.join(root, _file).replace("\\", "/")

                stat = self.fs.stat(path)

                path = posixpath.relpath(path, self.fs.basepath)

                self._files[path] = {
                    "size": stat.st_size,
                    "mdate": stat.st_mtime
                }
        
        return self

print("""
SYNC 1.0 - Script de synchronisation entre deux dossiers
--------------------------------------------------------""")

# Configuration des paramètres de la ligne de commande
parser = argparse.ArgumentParser(prog="sync",
    formatter_class=argparse.RawDescriptionHelpFormatter,
    description="Synchronise le contenu de deux dossiers (locaux ou FTP).",
    epilog="""
Les chemins réseaux de type \\\\serveur\\partage ne sont pas supportés. Pour les
dossiers hébergés sur un serveur FTP, le paramètre dirleft ou dirright doit
respecter le format d'url suivante : 
ftp://user:password@server/path/to/the/directory

Lorsque la synchronisation doit déterminer quel fichier est le plus récent
entre deux fichiers situés de chaque coté dans la même arborescence, elle se
base sur les règles suivantes :
- Le fichier le plus récent est celui avec la date de modification la plus
  récente.
- Si les dates de modification sont identiques, le fichier le plus récent est
  celui dont la taille est la plus grosse.
- Si les dates de modification et la taille sont égales, les fichiers sont
  considérés comme identiques et ne seront pas synchronisés.
""")

parser.add_argument("dirleft", help="Chemin absolu vers le dossier de gauche.")
parser.add_argument("dirright", help="Chemin absolu vers le dossier de droite.")
parser.add_argument(
    "--debug",
    action="store_true",
    dest="debug",
    help="""
Ajoute des informations supplémentaires dans le fichier journal. L'option
--no-log est alors ignorée.""")
parser.add_argument(
    "-l",
    "--log", 
    dest="logpath", 
    metavar="FILE",
    default="sync.log",
    help="""
Chemin vers le fichier journal. Par défaut, le fichier sync.log sera créé dans
le répertoire courant.""")
parser.add_argument(
    "-m", 
    "--mirroring", 
    dest="mirroring", 
    action="store_true",
    help="""
Exécute la synchronisation en mode mirroir. Tout le contenu de dirleft est copié
dans dirright. Les fichiers qui existaient uniquement dans dirright sont 
supprimés.""")
parser.add_argument(
    "--no-log", 
    dest="log_activated", 
    action="store_false",
    help="Aucun fichier journal ne sera créé.")
parser.add_argument(
    "--preserve-dirright", 
    action="store_true",
    dest="preserve_dirright", 
    help="""
Dans le cas d'une copie en mode miroir, les fichiers existant dans dirright et
absent de dirleft ne sont pas supprimés.""")
parser.add_argument("--version", action="version", version="%(prog)s 1.0")

print("\n> Analyse des paramètres de la ligne de commande...")
config = SyncConfiguration(parser)
print("Terminé.")

### Initialisation de la log
print("\n> Initialisation du module de log...")
log = logging.getLogger(__name__)

if config.debug:
    log.setLevel(logging.DEBUG)
else:
    log.setLevel(logging.INFO)

consoleLog = logging.StreamHandler()
consoleLog.setFormatter(logging.Formatter("%(message)s"))

if config.logActivated:
    fileLog = logging.FileHandler(config.logpath, mode="w")
    fileLog.setFormatter(
        logging.Formatter("%(asctime)s\t%(levelname)s\t%(message)s"))
    log.addHandler(fileLog)

log.addHandler(consoleLog)

print("Terminé.")

print("\n> Configuration du module de synchronisation")
print(config)

### Synchronisation des fichiers
sync = Sync(config, log)

# Mise a jour des statistiques du dossier de gauche
print("\n> Parcours de l'arborescence du dossier de gauche ({})...".format(
    sync.dirLeft))
sync.dirLeft.scan()

log.info("Le dossier '{path}' contient {nfiles} fichier(s) dans {ndirs} \
répertoire(s) ({size:.2f}Mo).".format(
    path=sync.dirLeft,
    nfiles=len(sync.dirLeft.files), 
    ndirs=len(sync.dirLeft.dirs),
    size=sync.dirLeft.size / 1048576))

# Mise a jour des statistiques du dossier de droite
print("\n> Parcours de l'arborescence du dossier de droite ({})...".format(
    sync.dirRight))
sync.dirRight.scan()

log.info("Le dossier '{path}' contient {nfiles} fichier(s) dans {ndirs} \
répertoire(s) ({size:.2f}Mo).".format(
    path=sync.dirRight,
    nfiles=len(sync.dirRight.files), 
    ndirs=len(sync.dirRight.dirs),
    size=sync.dirRight.size / 1048576))

# Mise à jour des informations de synchronisation
print("\n> Mise à jour des informations de synchronisation...")
sync.updateSyncInfos()
print("Terminé.")

# Synchronisation des dossiers
print("\n> Synchronisation des dossiers...")

try:
    sync.sync()
except Exception as e:
    log.error("{}".format(e))
    raise
    
print("Terminé.")
log.info("Synchronisation terminée.")
