#!/usr/bin/env python
# -*- coding: utf-8 -*-

'''\
Watchdog demo.

Requires watchdog library.
'''

from __future__ import unicode_literals

from argparse import ArgumentParser, RawDescriptionHelpFormatter
from hashlib import sha256
from logging import getLogger, StreamHandler, Formatter, NullHandler
from logging import DEBUG

import time
import os

from watchdog.events import FileSystemEventHandler
from watchdog.observers import Observer

_null_logger = getLogger(__name__)
_null_logger.addHandler(NullHandler())


def _calc_digest(path, logger=None):
    logger = logger or _null_logger
    logger.debug('Start calculating hexdigest for {}'.format(path))
    h = sha256()
    with open(path, 'rb') as f:
        for chunk in iter(lambda: f.read(4096), b''):
            h.update(chunk)
    logger.debug('Finished calculating hexdigest for {}'
                 .format(path))
    return h.hexdigest()


class FSChangeHandler(FileSystemEventHandler):
    def __init__(self, path_to_watch, logger=None, show_digest=False):
        self.path_to_watch = path_to_watch
        self.show_digest = show_digest
        self.logger = logger or _null_logger

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
        if self.show_digest:
            logger.info('"{}" has been created (sha256: {})'
                        .format(event.src_path,
                                _calc_digest(event.src_path)))
        else:
            logger.info('"{}" has been created.'.format(event.src_path))

    def on_modified(self, event, logger=None):
        if event.src_path == self.path_to_watch:
            return
        logger = logger if logger else self.logger
        if self.show_digest:
            logger.info('"{}" has been modified (sha256: {})'
                        .format(event.src_path,
                                _calc_digest(event.src_path)))
        else:
            logger.info('"{}" has been modified.'.format(event.src_path))

    def on_deleted(self, event, logger=None):
        if event.src_path == self.path_to_watch:
            return
        logger = logger if logger else self.logger
        logger.info('"{}" has been deleted.'.format(event.src_path))

    def on_moved(self, event, logger=None):
        if event.src_path == self.path_to_watch:
            return
        logger = logger if logger else self.logger
        logger.info('"{}" has been moved to "{}"'
                    .format(event.src_path, event.dest_path))


def main():
    parser = ArgumentParser(description=(__doc__),
                            formatter_class=RawDescriptionHelpFormatter)
    parser.add_argument('path_to_watch', help=('Path to watch'))
    parser.add_argument('--log',
                        default='INFO',
                        help=('Set log level. e.g. DEBUG, INFO, WARN'))
    parser.add_argument('-d', '--debug', action='store_true',
                        help=('Path to watch'))
    parser.add_argument('-s', '--show-digest', action='store_true',
                        help='Show hexdigest on file creation/modification')
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

    event_handler = FSChangeHandler(path_to_watch,
                                    logger=logger,
                                    show_digest=args.show_digest)
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
