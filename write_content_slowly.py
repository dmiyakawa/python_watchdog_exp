#!/usr/bin/env python
# -*- coding: utf-8 -*-

'''\
Write content slowly
'''

from __future__ import unicode_literals
from __future__ import division
from __future__ import absolute_import

from argparse import ArgumentParser, RawDescriptionHelpFormatter
from logging import getLogger, StreamHandler, Formatter
from logging import NOTSET, DEBUG, INFO, WARN, ERROR, CRITICAL
import os
import random
import string
import sys
import time

# Python 2.7以降では直接文字列を指定して期待するログレベルになるが
# 2.6では期待通りの挙動をしない (エラーにもならない)。
# ここでは2.6系でも動作するようにすること、また--logに適切な
# 文字列が来ているかを確認することを目的に、辞書を作っておく
_LOG_LEVELS = {'NOTSET': NOTSET, 'DEBUG': DEBUG, 'INFO': INFO, 'WARN': WARN,
               'ERROR': ERROR, 'CRITICAL': CRITICAL}


def main():
    parser = ArgumentParser(description=(__doc__),
                            formatter_class=RawDescriptionHelpFormatter)
    parser.add_argument('path', help='file path to create')
    parser.add_argument('--size', default=1024*1024,
                        help='Size of the created file (in bytes)')
    parser.add_argument('--log', default='INFO',
                        help=('Set log level.'
                              ' NOTSET, DEBUG, INFO, WARN, ERROR, CRITICAL'
                              ' is available'))
    parser.add_argument('-d', '--debug', action='store_true',
                        help='Same as --log DEBUG')
    parser.add_argument('-w', '--warn', action='store_true',
                        help='Same as --log WARN')
    args = parser.parse_args()

    logger = getLogger(__name__)
    handler = StreamHandler()
    logger.addHandler(handler)
    if args.debug:
        logger.setLevel(DEBUG)
        handler.setLevel(DEBUG)
    elif args.warn:
        logger.setLevel(WARN)
        handler.setLevel(WARN)
    else:
        if args.log.upper() not in _LOG_LEVELS:
            parser.print_help(file=sys.stderr)
            print('\nInvalid option specified', file=sys.stderr)
            sys.exit(1)
        log_level = _LOG_LEVELS[args.log.upper()]
        logger.setLevel(log_level)
        handler.setLevel(log_level)
    # e.g. '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    handler.setFormatter(Formatter('%(asctime)s %(message)s'))
    logger.info('Start Running')

    path = os.path.abspath(args.path)
    logger.info('Start creating a file "{}", slowly.'.format(path))
    remaining = args.size
    f = open(path, 'wb')
    try:
        while remaining > 0:
            size_to_write = random.randint(1024, 1024*100)
            logger.debug('Writing {} bytes'.format(size_to_write))
            if remaining < size_to_write:
                size_to_write = remaining
            t = ''.join(random.choice(string.printable)
                        for i in range(size_to_write))
            d = bytes(t, encoding='ascii')
            written_size = f.write(d)
            remaining -= written_size
            logger.debug('Written {} bytes. {} bytes remaining'
                         .format(written_size, remaining))
            time.sleep(1)
    finally:
        f.close()

    logger.info('Finished Running')


if __name__ == '__main__':
    main()
