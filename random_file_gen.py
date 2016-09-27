#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Python 3.5.2で動作検証済
#

'''\
watchdog2_main.py と対向するプログラム。
対象のディレクトリに無造作にブロブファイルを作成し、
作成したファイルを変更したり削除したりもする。
'''

from argparse import ArgumentParser, RawDescriptionHelpFormatter
from logging import getLogger, StreamHandler, Formatter, NullHandler
from logging import DEBUG

import os
import string
import random

_null_logger = getLogger(__name__)
_null_logger.addHandler(NullHandler())

EXTENSIONS = ['rtf', 'xls', 'xlsx', 'ppt', 'pptx',
              'pdf', 'bmp', 'jpg', 'gif', 'bmp', 'png',
              'zip']


def create_dirs(base_dir_path, max_depth, num_dirs, dirs,
                *, logger=None):
    logger = logger or _null_logger
    if not max_depth:
        return
    for dir_index in range(num_dirs):
        dir_name = ''.join(
            random.choice(string.ascii_letters) for l in range(8))
        dir_path = os.path.join(base_dir_path, dir_name)
        logger.info('Creating directory "{}"'.format(dir_path))
        os.mkdir(dir_path)
        # ファイルを保存する対象ディレクトリとして記憶
        dirs.append(dir_path)
        # 再帰的にディレクトリ作成
        create_dirs(dir_path, max_depth - 1, num_dirs, dirs)


def main():
    parser = ArgumentParser(description=(__doc__),
                            formatter_class=RawDescriptionHelpFormatter)
    parser.add_argument('--log',
                        default='INFO',
                        help=('Set log level. e.g. DEBUG, INFO, WARN'))
    parser.add_argument('path', help=('The path of target directory'))
    parser.add_argument('-d', '--debug', action='store_true',
                        help=('Path to watch'))
    parser.add_argument('-n', '--num-files', type=int, default=100,
                        help='Num of files to be generated')
    parser.add_argument('--max-depth', type=int, default=0,
                        help=('Possible directory depth.'
                              ' 0 means no directory will be created'))
    parser.add_argument('--num-dirs', type=int, default=100,
                        help=('Number of directories to be created per'
                              ' directory. This will be ignored when'
                              ' --max-depth is set to 0. Note that'
                              ' a child directory also has this number'
                              ' of directories.'
                              ' For example, if --max-depth is set to 2'
                              ' and --num-dirs is set to 100, num of'
                              ' total directories would become 100^2.'
                              ' Be careful when you are going to'
                              ' set this value very big.'
                              ' This tool does not care OS limitation.'))
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
    handler.setFormatter(Formatter('%(asctime)s %(message)s'))
    path = os.path.abspath(args.path)
    if not os.path.exists(path):
        logger.error('"{}" does not exist'.format(path))
        return
    if not os.path.isdir(args.path):
        logger.error('"{}" does is not a directory'.format(path))
        return
    logger.info('Started running (path: {})'.format(path))

    num_files = args.num_files
    max_depth = args.max_depth
    num_dirs = args.num_dirs
    dirs = []

    if max_depth > 0:
        logger.info('Start creating directories.')
        create_dirs(path, max_depth, num_dirs, dirs,
                    logger=logger)

    logger.info('Start creating files')
    for i in range(num_files):
        if dirs:
            dir_path = random.choice(dirs)
        else:
            dir_path = path

        filename = '{}.{}'.format(
            ''.join(random.choice(string.ascii_letters) for l in range(8)),
            random.choice(EXTENSIONS))
        file_path = os.path.join(dir_path, filename)
        logger.info('Creating file "{}" ({}/{})'
                    .format(file_path, i+1, num_files))
        f = open(file_path, 'w')
        num_bytes = 8 * 1024
        f.write(''.join(random.choice(string.ascii_letters)
                        for l in range(num_bytes)))
        f.close()

    logger.info('Finished running')


if __name__ in '__main__':
    main()
