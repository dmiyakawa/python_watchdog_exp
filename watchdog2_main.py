#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Python3 のみで動作可能
# Python 3.5.2で動作検証済
#

'''\
Watchdog demo. Requires watchdog library.

指定されたディレクトリを監視し、新たに追加されたファイルの内
特定の拡張子を持つものを対象にして監視ディレクトリから見た
ファイルの相対パスとsha1のhexdigestをsqlite3 DBに保存する。
'''

from argparse import ArgumentParser, RawDescriptionHelpFormatter
from logging import getLogger, StreamHandler, Formatter, NullHandler
from logging import DEBUG

import hashlib
import sqlite3
import time
import os

from watchdog.events import FileSystemEventHandler
from watchdog.observers import Observer

_null_logger = getLogger(__name__)
_null_logger.addHandler(NullHandler())

EXTENSIONS = {'.rtf', '.xls', '.xlsx', '.ppt', '.pptx',
              '.pdf', '.bmp', '.jpg', '.gif', '.bmp', '.png',
              '.zip'}

BASE_DIR = os.path.abspath(os.path.dirname(__file__))

SQLITE3_FILENAME = 'db.sqlite3'
SQLITE3_PATH = os.path.join(BASE_DIR, SQLITE3_FILENAME)


class DBRecorder(object):
    def __init__(self, db_path, base_dir_path, *, drop=False, logger=None):
        self.db_path = os.path.abspath(db_path)
        self.base_dir_path = base_dir_path
        self.logger = logger or _null_logger
        logger.info('Init DB at "{}"'.format(self.db_path))
        conn = self._connect()
        c = conn.cursor()
        if drop:
            c.execute('''\
            DROP TABLE IF EXISTS files
            ''')
        c.execute('''\
        CREATE TABLE IF NOT EXISTS
        files (filename text, sha1 text)
        ''')
        c.execute('''\
        CREATE UNIQUE INDEX IF NOT EXISTS
        files_index ON files (filename)
        ''')
        conn.commit()
        logger.info('Init db finished')

    def _connect(self):
        '''\
        DBに接続する。

        Note: watchdogのイベントは本体のスレッドとは
        別のスレッドから発行される。
        一方sqlite3のConnectionオブジェクトはスレッド間での使い回しが利かない。
        よって、DBRecorder全体でひとつのConnectionを共有するのではなく、
        必要なタイミングでコネクションを張り直す必要がある。
        '''
        # 標準のisolation_levelもDEFERREDのはずだが明文化されていない
        # 明示しておく。
        return sqlite3.connect(SQLITE3_PATH,
                               isolation_level='DEFERRED')

    def print_content_to_logger(self, *, logger=None):
        logger = logger or self.logger
        logger.info('Showing all entries in db')
        c = self._connect().cursor()
        logger.info('rowcount: {}'.format(c.rowcount))
        for row in c.execute('SELECT filename, sha1 FROM files'):
            logger.info('{}: {}'
                        .format(row[0], row[1]))
        logger.info('Showed all entries in db')

    def insert(self, path, *, logger=None):
        logger = logger or self.logger
        if os.path.abspath(path) == self.db_path:
            logger.debug(
                'Modification to db itself is ignored ({})'
                .format(path))
            return
        rel_path = os.path.relpath(os.path.abspath(path),
                                   self.base_dir_path)
        sha1digest = hashlib.sha1(rel_path.encode('utf-8')).hexdigest()
        logger.info('Saving "{}" with sha1 "{}"'
                    .format(rel_path, sha1digest))
        conn = self._connect()
        c = conn.cursor()
        c.execute('''\
        INSERT OR REPLACE INTO files
        (filename, sha1)
        VALUES (?, ?)
        ''', (rel_path, sha1digest))
        conn.commit()

    def delete(self, path, *, logger=None):
        logger = logger or self.logger
        if os.path.abspath(path) == self.db_path:
            logger.debug(
                'Modification to db itself is ignored ({})'
                .format(path))
            return
        rel_path = os.path.relpath(os.path.abspath(path),
                                   self.base_dir_path)
        logger.info('Deleting "{}"'.format(rel_path))
        conn = self._connect()
        c = conn.cursor()
        c.execute('DELETE FROM files WHERE (filename = ?)',
                  (rel_path,))
        conn.commit()


class FSChangeHandler(FileSystemEventHandler):
    def __init__(self, path_to_watch, recorder,
                 *, logger=None):
        self.path_to_watch = path_to_watch
        self.logger = logger or _null_logger
        self.recorder = recorder

    def on_any_event(self, event, logger=None):
        logger = logger or self.logger
        logger.debug(('on_any_event(type: {},'
                      ' path: {}, event_type: {},'
                      ' is_directory: {})')
                     .format(type(event),
                             event.src_path,
                             event.event_type,
                             event.is_directory))

    def on_created(self, event, logger=None):
        logger = logger or self.logger
        if event.is_directory:
            logger.debug('Ignoring "{}" because it is a directory'
                         .format(event.src_path))
            return
        (_, ext) = os.path.splitext(event.src_path)
        if ext not in EXTENSIONS:
            logger.debug(('Ignoring "{}" because it is ignorable according'
                          'to its extension "{}"')
                         .format(event.src_path, ext))
            return
        logger.info('"{}" has been created.'.format(event.src_path))
        self.recorder.insert(event.src_path, logger=logger)

    def on_modified(self, event, logger=None):
        logger = logger or self.logger
        if event.is_directory:
            logger.debug('Ignoring "{}" because it is a directory'
                         .format(event.src_path))
            return
        (_, ext) = os.path.splitext(event.src_path)
        if ext not in EXTENSIONS:
            logger.debug(('Ignoring "{}" because it is ignorable according'
                          'to its extension "{}"')
                         .format(event.src_path, ext))
            return
        logger.info('"{}" has been modified.'.format(event.src_path))
        self.recorder.insert(event.src_path, logger=logger)

    def on_deleted(self, event, logger=None):
        logger = logger or self.logger
        if event.is_directory:
            logger.debug('Ignoring "{}" because it is a directory'
                         .format(event.src_path))
            return
        (_, ext) = os.path.splitext(event.src_path)
        if ext not in EXTENSIONS:
            logger.debug(('Ignoring "{}" because it is ignorable according'
                          'to its extension "{}"')
                         .format(event.src_path, ext))
            return
        logger.info('"{}" has been deleted.'.format(event.src_path))
        self.recorder.delete(event.src_path, logger=logger)

    def on_moved(self, event, logger=None):
        logger = logger or self.logger
        if event.is_directory:
            logger.debug('Ignoring "{}" because it is a directory'
                         .format(event.src_path))
            return
        logger.info('"{}" has been moved to "{}"'
                    .format(event.src_path, event.dest_path))
        (_, ext) = os.path.splitext(event.src_path)
        # Note: deleteとinsertはアトミック操作である必要はない
        if ext not in EXTENSIONS:
            logger.debug(('Ignoring "{}" because it is ignorable according'
                          'to its extension "{}"')
                         .format(event.src_path, ext))
        else:
            self.recorder.delete(event.src_path, logger=logger)
        (_, ext) = os.path.splitext(event.dest_path)
        if ext not in EXTENSIONS:
            logger.debug(('Ignoring "{}" because it is ignorable according'
                          'to its extension "{}"')
                         .format(event.dest_path, ext))
        else:
            self.recorder.insert(event.dest_path, logger=logger)


def main():
    parser = ArgumentParser(description=(__doc__),
                            formatter_class=RawDescriptionHelpFormatter)
    parser.add_argument('--log',
                        default='INFO',
                        help=('Set log level. e.g. DEBUG, INFO, WARN'))
    parser.add_argument('-d', '--debug', action='store_true',
                        help=('Path to watch'))
    parser.add_argument('-p', '--path-to-watch', default=BASE_DIR,
                        help=('Path to watch'))
    parser.add_argument('-s', '--path-to-sqlite3', default=SQLITE3_PATH,
                        help=('Path to sqlite3 db'))
    parser.add_argument('--print-db-at-end', action='store_true',
                        help=('Print DB content to logger'
                              ' at the end of the execution'))
    parser.add_argument('--drop-table', action='store_true',
                        help=('If true, drop the sqlite3 table at first'))
    args = parser.parse_args()
    path_to_watch = os.path.abspath(args.path_to_watch)

    logger = getLogger(__name__)
    handler = StreamHandler()
    if args.debug:
        handler.setLevel(DEBUG)
        logger.setLevel(DEBUG)
    else:
        handler.setLevel(args.log.upper())
        logger.setLevel(args.log.upper())
    logger.addHandler(handler)
    # e.g. '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    handler.setFormatter(Formatter('%(asctime)s %(message)s'))
    logger.info('Started running (path: {})'.format(path_to_watch))

    recorder = DBRecorder(SQLITE3_PATH, path_to_watch, logger=logger)
    event_handler = FSChangeHandler(path_to_watch,
                                    recorder,
                                    logger=logger)
    observer = Observer()
    observer.schedule(event_handler, path_to_watch, recursive=True)
    observer.start()
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()
    observer.join()
    if args.print_db_at_end:
        recorder.print_content_to_logger()
    logger.info('Ended')


if __name__ in '__main__':
    main()
