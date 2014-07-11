# -*- coding:utf-8 -*-

from datetime import datetime

import ftplib
import ftputil
import pytz

class FTPSession(ftplib.FTP):
    def __init__(self, host, user, password, port=21):
        super(FTPSession, self).__init__()
        self.connect(host, port)
        self.login(user, password)
        self.encoding = "utf8"

class _StatMLSD(ftputil.stat._Stat):
    def _stat_results_from_dir(self, path):
        """
        Yield stat results extracted from the directory listing `path`.
        Omit the special entries for the directory itself and its parent
        directory.
        """
        lines = self._host_dir(path)
        # `cache` is the "high-level" `StatCache` object whereas
        # `cache._cache` is the "low-level" `LRUCache` object.
        cache = self._lstat_cache
        # Auto-grow cache if the cache up to now can't hold as many
        # entries as there are in the directory `path`.
        if cache._enabled and len(lines) >= cache._cache.size:
            new_size = int(math.ceil(1.1 * len(lines)))
            cache.resize(new_size)

        # Ajout pour date de modification plus précise grâce la commande MLSD
        files = dict()
        for _file, _facts in self._host._session.mlsd(path):
            files[_file] = _facts
        # Fin ajout

        # Yield stat results from lines.
        for line in lines:
            if self._parser.ignores_line(line):
                continue
            # For `listdir`, we are interested in just the names,
            # but we use the `time_shift` parameter to have the
            # correct timestamp values in the cache.
            stat_result = self._parser.parse_line(line,
                                                  self._host.time_shift())
            if stat_result._st_name in [self._host.curdir, self._host.pardir]:
                continue

            # Ajout pour insérer une date de modification plus précise grâce la 
            # commande MLSD
            list_stat_result = list(stat_result)
            mtime = files[stat_result._st_name]["modify"]
            mtimestamp = datetime.strptime(mtime, "%Y%m%d%H%M%S").timestamp()
            list_stat_result[8] = mtimestamp

            _stat_result = ftputil.stat.StatResult(list_stat_result)
            _stat_result._st_name = stat_result._st_name
            _stat_result._st_target = stat_result._st_target
            _stat_result._st_mtime_precision = stat_result._st_mtime_precision
            stat_result = _stat_result
            # Fin ajout
            loop_path = self._path.join(path, stat_result._st_name)
            self._lstat_cache[loop_path] = stat_result

            yield stat_result