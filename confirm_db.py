#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
#

from argparse import ArgumentParser, RawDescriptionHelpFormatter
from logging import getLogger, StreamHandler, Formatter, NullHandler
from logging import DEBUG

from collections import namedtuple
import hashlib
import os
import sqlite3
import time


InvalidPath = namedtuple('InvalidPath', ['path', 'reason'])

_null_logger = getLogger(__name__)
_null_logger.addHandler(NullHandler())


BASE_DIR = os.path.abspath(os.path.dirname(__file__))

SQLITE3_FILENAME = 'db.sqlite3'
SQLITE3_PATH = os.path.join(BASE_DIR, SQLITE3_FILENAME)


def _human_readable_time(elapsed_sec):
    elapsed_int = int(elapsed_sec)
    days = elapsed_int // 86400
    hours = (elapsed_int // 3600) % 24
    minutes = (elapsed_int // 60) % 60
    seconds = elapsed_int % 60
    if days > 0:
        return '{} days {:02d}:{:02d}:{:02d}'.format(days, hours,
                                                     minutes, seconds)
    return '{:02d}:{:02d}:{:02d}'.format(hours, minutes, seconds)


def main():
    parser = ArgumentParser(description=(__doc__),
                            formatter_class=RawDescriptionHelpFormatter)
    parser.add_argument('path_to_check', help=('Path to check'))
    parser.add_argument('--log',
                        default='INFO',
                        help=('Set log level. e.g. DEBUG, INFO, WARN'))
    parser.add_argument('-d', '--debug', action='store_true',
                        help=('Path to watch'))
    parser.add_argument('-p', '--path-to-sqlite3', default=SQLITE3_PATH,
                        help=('Path to sqlite3 db'))
    args = parser.parse_args()
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
    path_to_check = os.path.abspath(args.path_to_check)
    path_to_sqlite3 = os.path.abspath(args.path_to_sqlite3)

    handler.setFormatter(Formatter('%(asctime)s %(message)s'))
    try:
        logger.info('Started running')
        logger.info('path_to_check: "{}"'.format(path_to_check))
        logger.info('path_to_sqlite3: "{}"'.format(path_to_sqlite3))
        started = time.time()
        conn = sqlite3.connect(path_to_sqlite3)
        c = conn.cursor()
        invalid_paths = []
        total_files = 0
        for (dirpath, dirnames, filenames) in os.walk(path_to_check):
            for filename in filenames:
                total_files += 1
                file_path = os.path.join(dirpath, filename)
                rel_path = os.path.relpath(file_path, path_to_check)
                logger.debug('Checking "{}"'.format(rel_path))
                rows = c.execute('''\
                SELECT filename, sha1 FROM files
                WHERE filename = ?
                ''', (rel_path,)).fetchall()
                if len(rows) != 1:
                    reason = ('Multiple rows ({})'.format(len(rows)))
                    logger.debug(reason)
                    invalid_paths.append(InvalidPath(rel_path, reason))
                    continue
                (filename, actual_sha1) = rows[0][0], rows[0][1]
                assert filename == rel_path
                s = hashlib.sha1(rel_path.encode('utf-8'))
                expected_sha1 = s.hexdigest()
                if expected_sha1 != actual_sha1:
                    reason = ('sha1 differs (expected: "{}", actual: "{}"'
                              .format(expected_sha1, actual_sha1))
                    logger.debug(reason)
                    invalid_paths.append(InvalidPath(rel_path, reason))
                    continue
        logger.info('{} files handled'.format(total_files))
        if invalid_paths:
            logger.error('DB may contain insufficient or incorrect data')
            for invalid_path in invalid_paths:
                logger.error('"{}" ({})'.format(invalid_path.path,
                                                invalid_path.reason))
        else:
            logger.info('DB seems to have no incorrect data'
                        ' (Note: it may have data more than expected though)')
        conn.close()
        ended = time.time()
        logger.info('Finished running (Elapsed {})'
                    .format(_human_readable_time(ended - started)))
    except KeyboardInterrupt:
        logger.info('Keyboard Interrupt occured. Exitting')


if __name__ == '__main__':
    main()
