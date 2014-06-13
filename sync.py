# -*- coding:utf-8 -*-

import argparse
import logging
import os.path
import filesystem

class Sync:
    """Classe permettant de synchroniser deux répertoires"""

    def __init__(self, dirLeft, dirRight):
        self.setDirLeft(dirLeft)
        self.setDirRight(dirRight)
        self.__syncInfosUpdated = False

    def setDirLeft(self, path):
        self.dirLeft = SyncDirectory(path)
        self.__syncInfosUpdated = False
        
        return self

    def setDirRight(self, path):
        self.dirLeft = SyncDirectory(path)
        self.__syncInfosUpdated = False

        return self

    def sync(self):
        """Synchronization des fichiers."""

        if not self.__syncInfosUpToDate:
            self.updateSyncInfos()

        self.__sync()

        return self

    def updateSyncInfos(self):
        """Mise à jour des infos de synchronisation."""

        self.filesOnlyLeft = self.diff(self.dirLeft.files, self.dirRight.files)
        self.filesOnlyRight = self.diff(self.dirRight.files, self.dirLeft.files)
        self.filesLeftToBeCopied, self.filesRightToBeCopied = self.updateFilesToBeCopied()
        self.filesLeftToBeRemoved, self.filesRightToBeRemoved = self.updateFilesToBeRemoved()

        self.__syncInfosUpdated = True
        
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
            infos = infos + "Fichier log :" + self.logpath + "\n"

        if self.mirroring:
            infos = infos + "Mode miroir activé.\n"

        infos = infos + "Dossier gauche : " + self.dirLeft + "\n"\
            + "Dossier droite : " + self.dirRight + "\n"

        if self.mirroring and self.preserveDirRight:
            infos = infos + \
                "Les fichiers n'existant que dans le dossier de droite " + \
                "ne seront pas supprimés.\n"
    
        return infos

    def processArgs(self, parser):
        """Interprète les arguments de la ligne de commande."""
        args = parser.parse_args()

        self.debug = args.debug
        self.logpath = args.logpath
        self.mirroring = args.mirroring
        self.logActivated = args.log_activated
        self.preserveDirRight = args.preserve_dirright

        if os.path.isabs(args.dirleft):
            self.dirLeft = args.dirleft
        else:
            raise ValueError(
                "Erreur paramètre dirleft : Le chemin n'est pas absolu ({})."\
                .format(args.dirleft))

        if os.path.isabs(args.dirright):
            self.dirRight = args.dirright
        else:
            raise ValueError(
                "Erreur paramètre dirright : Le chemin n'est pas absolu ({})."\
                .format(args.dirright))

        return self

class SyncDirectory:
    def __init__(self, path):
        self.attachFileSystem(path)

    def __str__(self):
        return self.basepath

    def __getattr__(self, name):
        if name == "dirs":
            return __dirs()
        elif name == "files":
            return __files()
        elif name == "size":
            return __size()
        else
            raise AttributeError()

    def __dirs(self):
        """Liste des dossiers.

        Le renvoi se fait sous la forme d'un dictionnaire dont la clé est le
        chemin relatif du dossier et la valeur un dictionnaire contenant les
        clés suivantes : 
        - "size" : taille du dossier en octet
        - "mdate": date de modification du dossier en UTC (objet Datetime)
        """
        dirs = dict()

        for root, _dirs, _files in self.fs.walk():
            for _dir in _dirs:
                path = os.path.join(root, _dir)
                stat = self.fs.stat(path)

                path = os.path.relpath(self.basepath, path).replace("\\", "/")

                dirs[path] = {
                    "size": stat["size"],
                    "mdate": stat["mdate"]

        return dirs

    def __files(self):
        """Liste des fichiers.

        Le renvoi se fait sous la forme d'un dictionnaire dont la clé est le
        chemin relatif du fichier et la valeur un dictionnaire contenant les
        clés suivantes : 
        - "size" : taille du fichier en octet
        - "mdate": date de modification du fichier en UTC (objet Datetime)
        """
        files = dict()

        for root, _dirs, _files in self.fs.walk():
            for _file in _files:
                path = os.path.join(root, _file)
                stat = self.fs.stat(path)

                path = os.path.relpath(self.basepath, path).replace("\\", "/")

                dirs[path] = {
                    "size": stat["size"],
                    "mdate": stat["mdate"]

        return files

    def __size(self):
        """Taille du répertoire (en octet)."""

        size = 0

        for stat in self.files.values():
            size = size + stat["size"]

        return size

    def attachFileSystem(path):
        self.fs = filesystem.getFileSystem(path)
        self.basepath = self.fs.basepath

    def scan(self):
        self.fs.walk()
        
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

fileLog = logging.FileHandler(config.logpath)
fileLog.setFormatter(
    logging.Formatter("%(asctime)s\t%s(levelname)s\t%(message)s"))

log.addHandler(consoleLog)
log.addHandler(fileLog)
print("Terminé.")

print("\n> Configuration du module de synchronisation")
print(config)

### Synchronisation des fichiers
sync = Sync(config.dirLeft, config.dirRight)

# Mise a jour des statistiques du dossier de gauche
print("\n> Parcours de l'arborescence du dossier de gauche ({})...".format(
    sync.dirLeft))
sync.dirLeft.scan()
log.info("Le dossier de gauche contient {files} dans {dirs}")

