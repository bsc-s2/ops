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

import boto3
import yaml
from boto3.s3.transfer import TransferConfig
from botocore.client import Config
from pykit import humannum
from pykit import jobq

import sendmail

MB = 1024**2
GB = 1024**3

mega = 1024.0 * 1024.0

uploaded_lock = threading.RLock()
stat_lock = threading.RLock()
flock = threading.RLock()

uploaded_per_second = {
    'start_time': time.time(),
    'uploading': 0,
}

stat = {
    'bytes_uploaded': 0,
    'uploaded_files': 0,
    'uploaded_speed': 0,
    'start_time': time.time(),
    'end_time':   time.time(),

    'failed_files': {},
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
                if (uploaded_per_second['max_upload_bytes']
                        - uploaded_per_second['uploading']) > bytes_amount:
                    uploaded_per_second['uploading'] += bytes_amount
                    break

            time.sleep(0.01)

            logger.debug('about to sleep 10 millisecond to slow down, upload %d fn %s' % (
                bytes_amount, self.fn))



def to_unicode(s):
    if isinstance(s, str):
        return s.decode('utf-8')

    return s


def to_utf8(s):
    if isinstance(s, unicode):
        return s.encode('utf-8')

    return s


def try_to_unicode(s):
    if isinstance(s, unicode):
        return s

    try:
        s = s.decode('utf-8')
    except UnicodeDecodeError:
        s = s.decode('cp1252')

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

    conf['DATA_DIRS'] = [to_unicode(dir_name) for dir_name in conf['DATA_DIRS']]
    conf['LOG_DIR'] = to_unicode(conf['LOG_DIR'])

    return conf


def get_iso_now():
    datetime_now = datetime.datetime.utcnow()
    return datetime_now.strftime('%Y%m%dT%H%M%SZ')


def dir_iter(dir_name, base_len):
    q = []
    base_dir = dir_name.split('/')
    q.append(base_dir)

    while True:
        if len(q) == 0:
            break
        dir_parts = q.pop(0)

        files = os.listdir('/'.join(dir_parts))

        for f in files:
            _dir_parts = dir_parts[:]
            _dir_parts.append(f)

            _dir_parts = [try_to_unicode(d) for d in _dir_parts]

            if os.path.isdir('/'.join(_dir_parts)):
                q.append(_dir_parts)

        # ignore base dir
        if len(dir_parts) == base_len:
            continue

        yield dir_parts, base_len


def get_files_to_upload(dir_name, progress_file):

    files_mtime = {}

    if os.path.isfile(progress_file):
        with open(progress_file) as fd:

            for line in fd:
                try:
                    mtime, fn = line.strip().split(' ', 1)
                    mtime = int(mtime)
                except Exception as e:
                    logger.error(repr(e))

                files_mtime[fn] = mtime

    files_to_upload = {}

    for f in os.listdir(dir_name):

        file_name = os.path.join(dir_name, f)

        if not os.path.isfile(file_name):
            continue

        mtime = int(os.stat(file_name).st_mtime)
        if mtime == files_mtime.get(file_name):
            continue

        files_to_upload[file_name] = os.stat(file_name).st_size

    return files_to_upload


def upload_one_file(file_name, base_len, s3_client):
    file_parts = file_name.split('/')
    bucket_name = file_parts[base_len]
    key = os.path.join(cnf['KEY_PREFIX'], '/'.join(file_parts[base_len+1:]))

    bucket_name = cnf['BUCKETS_MAP'].get(bucket_name, bucket_name)

    info = {'local_size': os.stat(file_name).st_size}

    callback = None
    if cnf['ENABLE_BANDWIDTH']:
        callback = RestrictUploadSpeed(file_name)

    config = TransferConfig(multipart_threshold=4 * GB,
                            multipart_chunksize=512 * MB)

    extra_args = None
    if cnf['FILE_ACL']:
        extra_args = {'ACL': cnf['FILE_ACL']}

    s3_client.upload_file(Filename=file_name,
                          Bucket=bucket_name,
                          Key=key,
                          Config=config,
                          ExtraArgs=extra_args,
                          Callback=callback,
                          )

    logger.warn('have uploaded file %s' % file_name)

    resp = s3_client.head_object(
        Bucket=bucket_name,
        Key=key
    )

    logger.warn('have headed file %s' % file_name)

    status = resp['ResponseMetadata']['HTTPStatusCode']
    if status != 200:
        logger.error('failed to put object: %s %d' % (key, status))
        return

    info['bucket'] = bucket_name
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
        endpoint_url=cnf['ENDPOINT_URL'],
    )

    return client

def get_dir_progress_file_name(dir_parts):
    return os.path.join(cnf['CACHE_DIR'],
                    'cache-filename-for' + '_'.join(dir_parts) + '.upload_progress')

def get_total_progress_file_name(base_dir):
    return os.path.join(cnf['LOG_DIR'],
                    'upload-progress-for' + base_dir.replace('/', '_') + '.log')

def upload_one_directory(args):

    s3_client = boto_client()

    dir_parts, base_len = args
    dir_name = '/'.join(dir_parts)
    progress_file = get_dir_progress_file_name(dir_parts)

    files_to_upload = get_files_to_upload(dir_name, progress_file)
    progress_f = open(progress_file, 'a')
    total_progress_f = open(get_total_progress_file_name(dir_parts[:base_len]), 'a')

    print 'start to upload ' + dir_name
    logger.info('start to upload ' + dir_name)

    def _upload_file(file_name):

        if cnf['ENABLE_SCHEDULE']:
            check_schedule()

        logger.info('start to upload file: %s' % file_name)

        info = upload_one_file(file_name, base_len, s3_client)
        if info is None:
            return

        if info['local_size'] != info['resp_size']:
            logger.error(('file size not equal, local_size: %d,'
                          'response size: %d') % (info['local_size'],
                                                  info['resp_size']))
            return

        upload_time = get_iso_now()
        line = '%s %s %s %s %d %s\n' % (
            file_name, info['bucket'], info['file_key'], info['etag'],
            info['local_size'], upload_time)

        line = to_utf8(line)

        with flock:
            progress_f.write('{mtime} {fn}\n'.format(
                        mtime=int(os.stat(file_name).st_mtime),
                        fn=file_name,
                        ))
            total_progress_f.write(line)
            total_progress_f.flush()

        if cnf['CLEAR_FILES']:
            _remove(file_name)

        with stat_lock:
            stat['bytes_uploaded'] += info['local_size']
            stat['uploaded_files'] += 1

    def _upload_file_safe(file_name):
        try:
            return _upload_file(file_name)
        except Exception as e:
            with stat_lock:
                stat['failed_files'][file_name] = repr(e)

    files_to_upload = files_to_upload.items()
    files_to_upload.sort(key=lambda x: x[1], reverse=True)
    files_to_upload = [x[0] for x in files_to_upload]

    jobq.run(files_to_upload, [
             (_upload_file_safe, cnf['THREADS_NUM_FOR_FILE'])])

    progress_f.close()
    total_progress_f.close()

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
                time.sleep(0.1)
                continue

            last_report_tm = ts_now
            last_uploaded_bytes = stat['bytes_uploaded']

            report_str = ('stat: bytes uploaded: %dMB, '
                          'has uploaded files num: %d average speed: %fMB/s') % (
                            stat['bytes_uploaded'] / MB,
                            stat['uploaded_files'],
                            added_bytes / time_used / MB)

            stat['end_time'] = time.time()
            stat['uploaded_speed'] = (stat['bytes_uploaded']
                            / (stat['end_time']-stat['start_time']))

        logger.info(report_str)
        print report_str

        while cnf['REPORT_INTERVAL'] + ts_now - time.time() > 0 and not sess['stop']:
            time.sleep(0.1)


def run_once(dir_name):
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

    jobq.run(dir_iter(dir_name, base_len),
             [(upload_one_directory, cnf['THREADS_NUM_FOR_DIR'])])

    report_sess['stop'] = True

    report_th.join()

    with stat_lock:
        stat['end_time'] = time.time()
        stat['uploaded_speed'] = (stat['bytes_uploaded']
                        / (stat['end_time']-stat['start_time']))

    send_stat()

def send_stat():

    hh = humannum.humannum

    content = '<body>'
    content += ('<h2> Summary: </h2>'
            '<p> <b>uploaded bytes</b>: {bytes_uploaded} </p>'
            '<p> <b>uploaded files</b>: {uploaded_files} </p>'
            '<p> <b>uploaded speed</b>: {uploaded_speed} / s </p>'
            '<p> <b>start at</b>      : {start_time} </p>'
            '<p> <b>end at</b>        : {end_time} </p>'
            '<p> <b>spend</b>        : {spend} seconds </p>'
            '<p>  </p>'
            ).format(
                bytes_uploaded=hh(stat['bytes_uploaded']),
                uploaded_files=stat['uploaded_files'],
                uploaded_speed=hh(stat['uploaded_speed']),
                start_time=time.strftime('%Y-%m-%d %H-%M-%S',
                    time.localtime(stat['start_time'])),
                end_time=time.strftime('%Y-%m-%d %H-%M-%S',
                    time.localtime(stat['end_time'])),
                spend=int(stat['end_time'] - stat['start_time']),
            )

    failed_num = len(stat['failed_files'])

    if failed_num > 0:
        content += ('<h2> Failed Files: '
                    '<b style="color:red;">{n}</b></h2>').format(
                        n=failed_num)

        for file_name, error in stat['failed_files'].items():
            content += ('<h4> {file_name} : </h4>'
                        '<p> {error} </p>'
                ).format(
                    file_name=file_name,
                    error=error,
                )
    content += '</body>'

    subject = '[{tag}] Baishan Storage Daily Report {daily}'.format(
            tag='INFO' if failed_num <= 0 else 'WARN',
            daily=time.strftime('%Y-%m-%d', time.localtime()),
            )

    try:
        sendmail.send_mail(['sejust@163.com'], subject, content)
    except Exception as e:
        logger.exception(repr(e))

def run_dirs_once(dirs):

    jobq.run(dirs, [(run_once, 1)])

def run_forever(dirs):

    while True:

        prev_uploaded = stat['bytes_uploaded']

        run_dirs_once(dirs)

        if stat['bytes_uploaded'] - prev_uploaded == 0:
            time.sleep(60)

def upload_buckets(cnf):

    if cnf['RUN_FOREVER']:
        run_forever(cnf['DATA_DIRS'])
    else:
        run_dirs_once(cnf['DATA_DIRS'])


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


def add_logger(cnf):

    log_file = os.path.join(cnf['LOG_DIR'], 'upload-log-for' +
            '-'.join(cnf['DATA_DIRS'][:3]).replace('/', '_') + '.log')

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
        conf_path = '../conf/upload_buckets.yaml'
    else:
        conf_path = opts['--conf']

    cnf = get_conf(conf_path)

    uploaded_per_second['max_upload_bytes'] = float(
        cnf['BANDWIDTH']) * mega / 8

    _mkdir(cnf['LOG_DIR'])
    _mkdir(cnf['CACHE_DIR'])

    logger = add_logger(cnf)

    upload_buckets(cnf)
