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


def boto_client():
    session = boto3.session.Session()

    client = session.client(
        's3',
        use_ssl=False,
        aws_access_key_id=cnf['ACCESS_KEY'],
        aws_secret_access_key=cnf['SECRET_KEY'],
        config=Config(signature_version='s3v4'),
        region_name='us-east-1',
        endpoint_url=cnf['ENDPOINT'],
    )

    return client


def report(sess):

    last_report_tm = time.time()
    last_downloaded_bytes = stat['bytes_downloaded']

    while not sess['stop']:
        time.sleep(cnf['REPORT_INTERVAL'])

        ts_now = time.time()
        time_used = ts_now - last_report_tm
        last_report_tm = ts_now

        bytes_added = stat['bytes_downloaded'] - last_downloaded_bytes
        last_downloaded_bytes = stat['bytes_downloaded']

        current_speed = bytes_added / time_used / mega

        average_speed = stat['bytes_downloaded_this_period'] / \
            (stat['download_end_time'] - stat['download_start_time']) / mega

        report_str = ('bytes_downloaded: %.3f MB, current speed: %.3f MB/s, average_speed: %.3f MB/s' %
                      (stat['bytes_downloaded'] / mega, current_speed, average_speed))
        report_str += '\n' + ('total: %d, exist: %d, failed: %d' %
                              (stat['total'], stat['exist'], stat['failed']))

        logger.info(report_str)
        print report_str


def get_time_to_sleep():
    time_need = stat['bytes_downloaded_this_period'] / \
        (stat['bandwidth'] * mega)
    time_to_sleep = stat['download_start_time'] + time_need - time.time()

    return time_to_sleep


def iter_file():
    num_limit = cnf['NUM_LIMIT']

    start_marker = cnf['START_MARKER']
    end_marker = cnf['END_MARKER']

    marker = start_marker
    n = 0

    try:
        while True:
            resp = client.list_objects(
                Bucket=cnf['BUCKET_NAME'],
                Marker=marker,
                Prefix=cnf['PREFIX'],
            )

            if 'Contents' not in resp:
                break

            for content in resp['Contents']:
                if num_limit is not None and n >= num_limit:
                    return

                if end_marker is not None and content['Key'] >= end_marker:
                    return

                yield {
                    'key_name': content['Key'],
                    'etag': content['ETag'],
                    'last_modified': content['LastModified'],
                    'size': content['Size'],
                }

                n += 1
                marker = content['Key']

    except Exception as e:
        logger.error('failed to iter file: ' + traceback.format_exc())
        print 'failed to iter file: ' + repr(e)


def get_file_path(key_name):
    if not cnf['USE_FULL_NAME']:
        key_name = key_name[len(cnf['PREFIX']):]

    if cnf['PLAIN_DIRECTORY']:
        key_name = key_name.replace('/', cnf['PATH_DELIMITER'])

    key_name = key_name.lstrip('/')

    return os.path.join(cnf['DOWNLOAD_BASE_DIR'], key_name)


def check_local_file(file_path, result):
    is_dir = os.path.isdir(file_path)

    if file_path.endswith('/'):
        is_file = os.path.isfile(os.path.split(file_path)[0])
    else:
        is_file = os.path.isfile(file_path)

    if file_path.endswith('/'):
        if is_file:
            raise LocalFileError(
                'local file: %s should be a directory' % file_path[:-1])

        if not is_dir:
            _mkdir(file_path)
    else:
        if is_dir:
            raise LocalFileError(
                'local file: %s should be a file' % file_path)
        if not is_file:
            _mkdir(os.path.split(file_path)[0])

    if file_path.endswith('/'):
        logger.info('do not need to download, the file path is: ' + file_path)
        return False

    if is_file:
        file_stat = os.stat(file_path)

        local_mtime = file_stat.st_mtime
        local_size = file_stat.st_size

        remote_mtime = time.mktime(
            result['file_info']['last_modified'].utctimetuple()) + cnf['TIME_ZONE']
        remote_size = result['file_info']['size']

        if local_mtime > remote_mtime and local_size == remote_size:
            result['file_exists'] = True
            logger.info(
                'do not need to download, the file: %s exists' % file_path)
            return False

    return True


def download_file(file_info):
    result = {
        'file_info': file_info,
    }
    try:
        if cnf['ENABLE_SCHEDULE']:
            check_schedule()

        time_to_sleep = get_time_to_sleep()
        if time_to_sleep > 0:
            logger.info('about to sleep %.3f seconds to slow down' %
                        time_to_sleep)
            time.sleep(time_to_sleep)

        file_path = get_file_path(file_info['key_name'])

        if file_path.endswith('/') and file_info['size'] != 0:
            raise FileContentError(
                'the length of file: %s is not 0' % file_info['key_name'])

        need_to_download = check_local_file(file_path, result)
        if not need_to_download:
            return result

        logger.info('start download file: %s to path: %s' %
                    (file_info['key_name'], file_path))

        client.download_file(cnf['BUCKET_NAME'],
                             file_info['key_name'], file_path)

        return result

    except Exception as e:
        logger.error('failed to download file: %s, %s' %
                     (repr(file_info), traceback.format_exc()))
        print 'failed to download file: %s, %s' % (repr(file_info), repr(e))
        result['error'] = True
        return result


def update_stat(result):
    with stat_lock:
        stat['total'] += 1

        if result.get('error') == True:
            stat['failed'] += 1
            return

        if result.get('file_exists') == True:
            stat['exist'] += 1
            return

        stat['bytes_downloaded'] += result['file_info']['size']
        stat['bytes_downloaded_this_period'] += result['file_info']['size']
        stat['download_end_time'] = time.time()


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

    report_th = _thread(report, (report_sess,))

    jobq.run(iter_file(),
             [(download_file, cnf['THREADS_NUM_FOR_DOWNLOAD']),
              (update_stat, 1),
              ])

    report_sess['stop'] = True

    report_th.join()


def run_forever():

    while True:
        run_one_turn()
        time.sleep(60)


def check_schedule():
    start_h = int(cnf['SCHEDULE_START'].split(':')[0])
    start_m = int(cnf['SCHEDULE_START'].split(':')[1])
    stop_h = int(cnf['SCHEDULE_STOP'].split(':')[0])
    stop_m = int(cnf['SCHEDULE_STOP'].split(':')[1])

    start_m = start_m + start_h * 60
    stop_m = stop_m + stop_h * 60

    while True:
        now = datetime.datetime.now()
        now_h = now.hour
        now_m = now.minute

        now_m = now_m + now_h * 60

        if start_m < stop_m:
            if now_m >= start_m and now_m <= stop_m:
                return
            else:
                wait_m = (start_m - now_m) % (60 * 24)
                line = ('the schedule is from %s to %s,'
                        ' need to wait %d hours and %d minutes') % (
                    cnf['SCHEDULE_START'], cnf['SCHEDULE_STOP'],
                    wait_m / 60, wait_m % 60)

                print line
                logger.warn(line)
                time.sleep(60)

                with stat_lock:
                    stat['download_start_time'] = time.time()
                    stat['bytes_downloaded_this_period'] = 0

        else:
            if now_m > stop_m and now_m < start_m:
                wait_m = (start_m - now_m) % (60 * 24)
                line = ('the schedule is from %s to %s,'
                        ' need to wait %d hours and %d minutes') % (
                    cnf['SCHEDULE_START'], cnf['SCHEDULE_STOP'],
                    wait_m / 60, wait_m % 60)

                print line
                logger.warn(line)
                time.sleep(60)

                with stat_lock:
                    stat['download_start_time'] = time.time()
                    stat['bytes_downloaded_this_period'] = 0
            else:
                return


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

    client = boto_client()

    stat['bandwidth'] = float(cnf['BANDWIDTH'])

    _mkdir(cnf['LOG_DIR'])

    logger = add_logger()

    if cnf['RUN_FOREVER']:
        run_forever()
    else:
        run_one_turn()
