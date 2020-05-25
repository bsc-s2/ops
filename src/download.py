#!/usr/bin/env python2
# coding: utf-8

import datetime
import errno
import getopt
import logging
import os
import sys
import threading
import time
import traceback

import boto3
import yaml
from botocore.client import Config
from pykit import http

from pykit import jobq

MB = 1024.0**2
GB = 1024.0**3

mega = 1024.0 * 1024.0

stat = {
    'bytes_downloaded': 0,
    'download_start_time': time.time(),
    'download_end_time': time.time(),
    'bytes_downloaded_this_period': 0,
    'bandwidth': 10,  # 10M
    'exist': 0,
    'failed': 0,
    'total': 0,
}

stat_lock = threading.RLock()


class FileContentError(Exception):
    pass


class LocalFileError(Exception):
    pass


def _thread(func, args):
    th = threading.Thread(target=func, args=args)
    th.daemon = True
    th.start()

    return th


def _mkdir(path):
    print path
    try:
        os.makedirs(path, 0755)
    except OSError as e:
        if e[0] == errno.EEXIST and os.path.isdir(path):
            pass
        else:
            raise


def get_conf(conf_path):

    with open(conf_path) as f:
        conf = yaml.safe_load(f.read())

    return conf


def iter_file_url(file_path):
    try:
        with open(file_path, 'r') as read_fp:
            while True:
                line = read_fp.readline().strip()
                if not line:
                    break

                yield line
    except Exception as e:
        logger.exception(repr(e) + ' when read files')


def need_to_download(file_path, result):
    if not os.path.isfile(file_path):
        return True

    file_stat = os.stat(file_path)
    return int(file_stat.st_size) != int(result['content-length'])


def try_download_file(file_path_url):
    host, uri = file_path_url.split(":")[:]

    cli = http.Client(host, 8008, timeout=300)
    cli.request(uri, method='GET')

    if cli.status != 200:
        raise 'status not 200'

    file_path = os.path.join('/cache3/ovsfile', uri[1:])

    is_need = need_to_download(file_path, cli.headers)
    if not is_need:
        logger.info('not download file: %s' % (file_path_url))
        return

    tmp_file_path = os.path.join('/cache3/ovsfile/tmp', uri[1:])

    with open(tmp_file_path, 'w', 0644) as fp:
        while True:
            buf = cli.read_body(1024*1024*1)
            if buf == '':
                break

            fp.write(buf)

        fp.flush()
        os.fsync(fp.fileno())

    os.rename(tmp_file_path, file_path)

def download_file(file_path_url):
    n = 0

    while  n < 10:
        try:
            logger.info('start download file: %s' % (file_path_url))

            try_download_file(file_path_url)

            logger.info('finish download file:%s' % (file_path_url))

            return
        except Exception as e:
            logger.exception(repr(e) + "while download file" + file_path_url)

        n += 1


def run_one_turn():

    logger.warn('one turn started')
    print 'one turn started'

    report_sess = {'stop': False}

    with stat_lock:
        stat['download_start_time'] = time.time()
        stat['bytes_downloaded_this_period'] = 0

        stat['bytes_downloaded'] = 0
        stat['exist'] = 0
        stat['failed'] = 0
        stat['total'] = 0

    jobq.run(iter_file_url(cnf['file_path']),
             [(download_file, cnf['THREADS_NUM_FOR_DOWNLOAD']),
              ])

    logger.warn('finished')


def run_forever():

    while True:
        run_one_turn()
        time.sleep(60)



def add_logger():

    log_file = os.path.join(cnf['LOG_DIR'], 'download-log-for-' +
                            cnf['BUCKET_NAME'] + '.log')

    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    file_handler = logging.FileHandler(log_file)
    formatter = logging.Formatter('[%(asctime)s, %(levelname)s] %(message)s')

    file_handler.setFormatter(formatter)

    logger.addHandler(file_handler)

    return logger


if __name__ == "__main__":

    opts, args = getopt.getopt(sys.argv[1:], '', ['conf=', ])
    opts = dict(opts)

    if opts.get('--conf') is None:
        conf_path = '../conf/download.yaml'
    else:
        conf_path = opts['--conf']

    cnf = get_conf(conf_path)

    print cnf
    _mkdir(cnf['LOG_DIR'])

    logger = add_logger()

    if cnf['RUN_FOREVER']:
        run_forever()
    else:
        run_one_turn()
