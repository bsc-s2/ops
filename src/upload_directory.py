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

import yaml

import boto3
from boto3.s3.transfer import TransferConfig
from botocore.client import Config
from pykit import jobq

MB = 1024**2
GB = 1024**3

mega = 1024.0 * 1024.0

uploaded_lock = threading.RLock()

uploaded_per_second = {
    'start_time': time.time(),
    'uploading': 0,
}


class RestrictUploadSpeed(object):

    def __init__(self, fn):
        self.fn = fn

    def __call__(self, bytes_amount):

        while True:

            curr_tm = time.time()

            with uploaded_lock:
                if curr_tm - uploaded_per_second['start_time'] > 1:
                    uploaded_per_second['start_time'] = curr_tm
                    uploaded_per_second['uploading'] = 0

            with uploaded_lock:
                if uploaded_per_second['max_upload_bytes'] - uploaded_per_second['uploading'] > bytes_amount:
                    uploaded_per_second['uploading'] += bytes_amount
                    break

            time.sleep(0.01)

            logger.debug('about to sleep 10 millisecond to slow down, upload %d fn %s' % (
                bytes_amount, self.fn))


stat = {
    'bytes_uploaded': 0,
    'uploaded_files': 0,
    'start_time': time.time(),
}

stat_lock = threading.RLock()
flock = threading.RLock()


def to_unicode(s):
    if isinstance(s, str):
        return s.decode('utf-8')

    return s


def to_utf8(s):
    if isinstance(s, unicode):
        return s.encode('utf-8')

    return s


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


def _remove(path):
    try:
        os.remove(path)
    except OSError as e:
        if e[0] == errno.ENOENT or os.path.isdir(path):
            pass
        else:
            raise


def get_conf(conf_path):

    with open(conf_path) as f:
        conf = yaml.safe_load(f.read())

    conf['DATA_DIR'] = to_unicode(conf['DATA_DIR'])
    conf['LOG_DIR'] = to_unicode(conf['LOG_DIR'])

    return conf


def is_visible_dir(dir_name):
    if dir_name.startswith('.'):
        return False

    return True


def is_visible_file(file_name):
    if file_name.startswith('.'):
        return False

    return True


def get_iso_now():
    datetime_now = datetime.datetime.utcnow()
    return datetime_now.strftime('%Y%m%dT%H%M%SZ')


def dir_iter(dir_name, base_len, key_prefix):
    q = []
    base_dir = dir_name.split('/')
    q.append(base_dir)

    while True:
        if len(q) < 1:
            break
        dir_parts = q.pop(0)

        files = os.listdir('/'.join(dir_parts))

        for f in files:
            _dir_parts = dir_parts[:]
            _dir_parts.append(f)

            if not is_visible_dir(f):
                continue

            parts = []
            for d in _dir_parts:
                if isinstance(d, unicode):
                    parts.append(d)
                    continue

                try:
                    d = d.decode('utf-8')
                except UnicodeDecodeError:
                    d = d.decode('cp1252')

                parts.append(d)

            if os.path.isdir('/'.join(parts)):
                q.append(parts)

        yield dir_parts, base_len, key_prefix


def get_files_to_upload(dir_name, progress_file):

    files = os.listdir(dir_name)
    files_to_upload = {}

    for f in files:
        if not is_visible_file(f):
            continue

        file_name = os.path.join(dir_name, f)

        if os.path.isfile(file_name):
            files_to_upload[file_name] = True

    fd = open(progress_file, 'a')
    fd.close()

    fd = open(progress_file)
    while True:
        line = fd.readline()
        if line == '':
            break

        file_name = line.split()[0].decode('utf-8')
        if file_name in files_to_upload:
            files_to_upload.pop(file_name)

    fd.close()

    return files_to_upload


def upload_one_file(file_name, base_len, key_prefix, s3_client):
    file_parts = file_name.split('/')
    key = os.path.join(key_prefix, '/'.join(file_parts[base_len:]))

    info = {'local_size': os.stat(file_name).st_size, }

    callback = None
    if cnf['ENABLE_BANDWIDTH']:
        callback = RestrictUploadSpeed(file_name)

    config = TransferConfig(multipart_threshold=4 * GB,
                            multipart_chunksize=512 * MB)

    s3_client.upload_file(Filename=file_name,
                          Bucket=cnf['BUCKET_NAME'],
                          Key=key,
                          Config=config,
                          ExtraArgs={'ACL': cnf['FILE_ACL']},
                          Callback=callback,
                          )

    logger.warn('have uploaded file %s' % file_name)

    resp = s3_client.head_object(
        Bucket=cnf['BUCKET_NAME'],
        Key=key
    )

    logger.warn('have headed file %s' % file_name)

    status = resp['ResponseMetadata']['HTTPStatusCode']
    if status != 200:
        logger.error('failed to put object: %s %d' % (key, status))
        return

    info['file_key'] = key
    info['etag'] = resp['ETag']
    info['resp_size'] = resp['ContentLength']

    info['upload_time'] = get_iso_now()

    return info


def boto_client():
    session = boto3.session.Session()

    client = session.client(
        's3',
        use_ssl=False,
        aws_access_key_id=cnf['ACCESS_KEY'],
        aws_secret_access_key=cnf['SECRET_KEY'],
        config=Config(signature_version='s3v4'),
        region_name='us-east-1',
        endpoint_url='http://s2.i.qingcdn.com',
    )

    return client


def upload_one_directory(args):

    s3_client = boto_client()

    dir_parts, base_len, key_prefix = args
    dir_name = '/'.join(dir_parts)
    progress_file = os.path.join(dir_name, '.upload_progress')

    files_to_upload = get_files_to_upload(dir_name, progress_file)
    progress_f = open(progress_file, 'a')

    print 'start to upload ' + dir_name
    logger.info('start to upload ' + dir_name)

    def _upload_file(file_name):

        if cnf['ENABLE_SCHEDULE']:
            check_schedule()

        logger.info('start to upload file: %s' % file_name)

        info = upload_one_file(file_name, base_len, key_prefix, s3_client)
        if info is None:
            return

        if info['local_size'] != info['resp_size']:
            logger.error(('file size not equal, local_size: %d,'
                          'response size: %d') % (info['local_size'],
                                                  info['resp_size']))
            return

        upload_time = get_iso_now()
        line = '%s %s %s %d %s\n' % (
            file_name, info['file_key'], info['etag'],
            info['local_size'], upload_time)

        line = to_utf8(line)

        with flock:
            progress_f.write(line)
            total_progress_f.write(line)
            total_progress_f.flush()

        if cnf['CLEAR_FILES']:
            _remove(file_name)

        with stat_lock:
            stat['bytes_uploaded'] += info['local_size']
            stat['uploaded_files'] += 1

    jobq.run(files_to_upload.keys(), [
             (_upload_file, cnf['THREADS_NUM_FOR_FILE'])])

    progress_f.close()

    print 'finish to upload ' + dir_name
    logger.info('finish to upload ' + dir_name)


def report(sess):

    last_report_tm = time.time()
    last_uploaded_bytes = stat['bytes_uploaded']

    while not sess['stop']:

        ts_now = time.time()

        with stat_lock:

            time_used = ts_now - last_report_tm
            added_bytes = stat['bytes_uploaded'] - last_uploaded_bytes

            if added_bytes == 0 or time_used == 0:
                continue

            last_report_tm = ts_now
            last_uploaded_bytes = stat['bytes_uploaded']

            report_str = ('stat: bytes uploaded: %dMB, has uploaded files num: %d average speed: %fMB/s') % (
                stat['bytes_uploaded'] / MB, stat['uploaded_files'], added_bytes / time_used / MB)

        logger.info(report_str)
        print report_str

        time.sleep(cnf['REPORT_INTERVAL'])


def run_once(dir_name, key_prefix):
    if dir_name.endswith('/'):
        print 'do not add / to the directory name: ' + dir_name
        return

    if not dir_name.startswith('/'):
        print 'the directory name is not absolute path: ' + dir_name
        return

    if not os.path.exists(dir_name) or not os.path.isdir(dir_name):
        print dir_name + ' is not exists or is not a directory'
        return

    base_len = len(dir_name.split('/'))

    report_sess = {'stop': False}

    report_th = _thread(report, (report_sess,))

    jobq.run(dir_iter(dir_name, base_len, key_prefix),
             [(upload_one_directory, cnf['THREADS_NUM_FOR_DIR'])])

    report_sess['stop'] = True

    report_th.join()


def run_forever(dir_name, key_prefix):

    while True:

        prev_uploaded = stat['bytes_uploaded']

        run_once(dir_name, key_prefix)

        if stat['bytes_uploaded'] - prev_uploaded == 0:
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
            else:
                return


def add_logger():

    log_file = os.path.join(cnf['LOG_DIR'], 'upload-log-for' +
                            cnf['DATA_DIR'].replace('/', '_') + '.log')

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
        conf_path = '../conf/upload_directory.yaml'
    else:
        conf_path = opts['--conf']

    cnf = get_conf(conf_path)

    uploaded_per_second['max_upload_bytes'] = float(
        cnf['BANDWIDTH']) * mega / 8

    _mkdir(cnf['LOG_DIR'])

    logger = add_logger()

    fn = os.path.join(cnf['LOG_DIR'], 'upload-progress-for' +
                      cnf['DATA_DIR'].replace('/', '_') + '.log')
    total_progress_f = open(fn, 'a')

    if cnf['RUN_FOREVER']:
        run_forever(cnf['DATA_DIR'], cnf['KEY_PREFIX'])
    else:
        run_once(cnf['DATA_DIR'], cnf['KEY_PREFIX'])

    total_progress_f.close()
