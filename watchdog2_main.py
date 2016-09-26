#!/usr/bin/env python
# -*- coding: utf-8 -*-

'''\
Watchdog demo.

Requires watchdog library.
'''

from __future__ import unicode_literals

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

BASE_DIR = os.path.abspath(os.path.dirname(__file__))

SQLITE3_FILENAME = 'db.sqlite3'
SQLITE3_PATH = os.path.join(BASE_DIR, SQLITE3_FILENAME)


class FSChangeHandler(FileSystemEventHandler):
    def __init__(self, path_to_watch, logger=None):
        self.path_to_watch = path_to_watch
        self.logger = logger or _null_logger
        self._init_db()

    def _init_db(self, logger=None):
        logger = logger if logger else self.logger
        logger.info('Init db at "{}"'.format(SQLITE3_PATH))
        conn = sqlite3.connect(SQLITE3_PATH)
        c = conn.cursor()
        c.execute('''\
        CREATE TABLE IF NOT EXISTS
        files (filename text, sha1 text)
        ''')
        c.execute('''\
        CREATE UNIQUE INDEX IF NOT EXISTS
        files_index ON files (filename)
        ''')
        conn.commit()

        c = conn.cursor()
        logger.debug('Showing all entries in db')
        for row in c.execute('SELECT filename, sha1 FROM files'):
            logger.debug('{}: {}'
                         .format(row[0], row[1]))
        logger.debug('Showed all entries in db')

        conn.close()
        logger.info('Init db finished')

    def on_any_event(self, event, logger=None):
        logger = logger if logger else self.logger
        logger.debug(('on_any_event(type: {},'
                      ' path: {}, event_type: {},'
                      ' is_directory: {})')
                     .format(type(event),
                             event.src_path,
                             event.event_type,
                             event.is_directory))

    def on_created(self, event, logger=None):
        if event.src_path == self.path_to_watch:
            return
        logger = logger if logger else self.logger
        logger.info('"{}" has been created.'.format(event.src_path))
        self._save_to_db(event.src_path, logger)

    def on_modified(self, event, logger=None):
        if event.src_path == self.path_to_watch:
            return
        logger = logger if logger else self.logger
        logger.info('"{}" has been modified.'.format(event.src_path))
        self._save_to_db(event.src_path, logger)

    def on_deleted(self, event, logger=None):
        if event.src_path == self.path_to_watch:
            return
        logger = logger if logger else self.logger
        logger.info('"{}" has been deleted.'.format(event.src_path))
        # TODO: delete entry

    def on_moved(self, event, logger=None):
        if event.src_path == self.path_to_watch:
            return
        logger = logger if logger else self.logger
        logger.info('"{}" has been moved to "{}"'
                    .format(event.src_path, event.dest_path))
        # TODO: delete source path
        self._save_to_db(event.dest_path, logger)

    def _save_to_db(self, path, logger=None):
        if path.endswith(SQLITE3_FILENAME):
            logger.debug('Modification to db itself is ignored')
            return
        logger = logger if logger else self.logger
        rel_path = os.path.relpath(os.path.abspath(path),
                                   self.path_to_watch)
        sha1digest = hashlib.sha1(rel_path.encode('utf-8')).hexdigest()
        logger.info('Saving "{}" with sha1 "{}"'
                    .format(rel_path, sha1digest))
        conn = sqlite3.connect(SQLITE3_PATH)
        c = conn.cursor()
        c.execute('''\
        INSERT OR REPLACE INTO files
        (filename, sha1)
        VALUES ("{}", "{}")
        '''.format(rel_path, sha1digest))
        conn.commit()
        conn.close()


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

    event_handler = FSChangeHandler(path_to_watch, logger=logger)
    observer = Observer()
    observer.schedule(event_handler, path_to_watch, recursive=True)
    observer.start()
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()
    observer.join()

    logger.info('Ended')


if __name__ in '__main__':
    main()
