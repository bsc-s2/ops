#!/usr/bin/env python2
# coding:utf-8

import argparse
import os
import sys
import threading
import time

import yaml

import file_filter
import token_bucket
import util
from pykit import http
from pykit import fsutil
from pykit import jobq
from pykit import threadutil

MB = 1024 ** 2


THREAD_STATUS = {}

ITER_STATUS = {
    'iter_n': 0,
    'iter_size': 0,
    'marker': '',
}


DOWNLOAD_STATUS = {
    'total_n': 0,
    'total_size': 0,
    'exception_n': 0,
    'exception_size': 0,
    'not_need_n': 0,
    'not_need_size': 0,
    'download_failed_n': 0,
    'download_failed_size': 0,
    'check_failed_n': 0,
    'check_failed_size': 0,
}

REAL_SPEED = [0] * 10


class LocalFileError(Exception):
    pass


class S3FileSizeError(Exception):
    pass


class S3GetError(Exception):
    pass


def iter_file():
    prefix = cnf.get('PREFIX', '')
    marker = cnf.get('MARKER', '')
    end_marker = cnf.get('END_MARKER', None)
    filter_conf = cnf.get('FILTER_CONF', None)

    logger.info('start to iter file from: ' + repr(marker))

    try:
        for content in util.iter_file(s3_client, cnf['BUCKET_NAME'],
                                      prefix=prefix, marker=marker):
            key_name = content['Key']

            if isinstance(key_name, unicode):
                key_name = key_name.encode('utf-8')

            if end_marker is not None and key_name >= end_marker:
                logger.info('iter file end, end maker: %s reached' %
                            end_marker)
                return

            ITER_STATUS['iter_n'] += 1
            ITER_STATUS['iter_size'] += content['Size']
            ITER_STATUS['marker'] = key_name

            file_object = {
                's3_key': key_name,
                'key': util.to_utf8(key_name,
                                    encodings=cnf.get('ENCODINGS', [])),
                'etag': content['ETag'].lower().strip('"'),
                'last_modified': content['LastModified'],
                'size': content['Size'],
            }

            msg = file_filter.filter(file_object, filter_conf, cnf['TIMEZONE'])

            if msg != None:
                logger.info('will not download file: %s, because: %s' %
                            (file_object['key'], msg))
                continue

            yield file_object

        logger.info('finished to iter file')

    except Exception as e:
        logger.exception('failed to iter file: ' + repr(e))


def get_local_path(key_name):
    if isinstance(key_name, unicode):
        key_name = key_name.encode('utf-8')

    if not cnf['USE_FULL_NAME']:
        key_name = key_name[len(cnf['PREFIX']):]

    if cnf['PLAIN_DIRECTORY']:
        key_name = key_name.replace('/', cnf['PATH_DELIMITER'])

    key_name = key_name.lstrip('/')

    return os.path.join(cnf['DOWNLOAD_BASE_DIR'], key_name)


def check_if_need_download(result, th_status):
    log_prefix = result['log_prefix']
    key = result['file_object']['key']
    s3_key = result['file_object']['s3_key']

    if s3_key.endswith('/'):
        if result['file_object']['size'] != 0:
            raise S3FileSizeError('size of file: %s, is not 0' % key)

        logger.info('%s do not need to download directory file: %s' %
                    (log_prefix, key))
        return False

    local_path = result['local_path']

    if os.path.isdir(local_path):
        raise LocalFileError('local path: %s, is a dir, key is: %s' %
                             (local_path, key))

    if not os.path.isfile(local_path):
        fsutil.makedirs(os.path.split(local_path)[0])
        return True

    file_stat = os.stat(local_path)

    local_size = file_stat.st_size
    remote_size = result['file_object']['size']

    if local_size != remote_size:
        return True

    local_mtime = file_stat.st_mtime

    remote_last_modified = result['file_object']['last_modified']
    remote_mtime = time.mktime(
        remote_last_modified.utctimetuple()) + cnf['TIMEZONE']

    if local_mtime < remote_mtime:
        return True

    if cnf['OVERRIDE']:
        th_status['override'] = th_status.get('override', 0) + 1
        return True

    logger.info('%s do not need to download exist file: %s' %
                (log_prefix, key))
    return False


def download_to_file(result, http_conn, th_status):
    log_prefix = result['log_prefix']
    local_path = result['local_path']
    key = result['file_object']['key']

    f = open(local_path, 'wb')

    body_size = int(http_conn.headers['content-length'])
    download_size = 0

    while True:
        tokens = min(body_size - download_size, cnf['DOWNLOAD_CHUNK_SIZE'])
        tb.get_tokens(tokens)

        curr_second = int(time.time())
        index = curr_second % len(REAL_SPEED)
        next_index = (curr_second + 1) % len(REAL_SPEED)

        REAL_SPEED[next_index] = 0
        REAL_SPEED[index] += tokens

        buf = http_conn.read_body(tokens)

        logger.info('%s download %d bytes for file: %s' %
                    (log_prefix, len(buf), key))

        if len(buf) != tokens:
            logger.error('%s read body error, tokens: %d, buf length: %d' %
                         (log_prefix, tokens, len(buf)))
            th_status['read_body_error'] = th_status.get(
                'read_body_error', 0) + 1
            raise S3GetError('read body error')

        f.write(buf)

        download_size += tokens

        th_status['progress'] = (
            download_size, body_size, (download_size + 1.0) / (body_size + 1))

        if download_size == body_size:
            break

    f.close()
    return


def download_data(result, th_status):
    log_prefix = result['log_prefix']
    s3_key = result['file_object']['s3_key']

    signed_url = s3_client.generate_presigned_url(
        'get_object',
        Params={
            'Bucket': cnf['BUCKET_NAME'],
            'Key': util.to_unicode(s3_key,
                                   encodings=cnf.get('ENCODINGS', [])),
        },
        ExpiresIn=60 * 60 * 12,
    )

    host, port, uri = util.parse_url(signed_url)

    h = http.Client(host, port, timeout=60 * 60)
    h.send_request(uri)
    h.read_response()

    if h.status != 200:
        error_msg = h.read_body(1024)
        logger.error('%s got invalid response from s3: %s, %s' %
                     (log_prefix, h.status, error_msg))
        raise S3GetError('invalid s3 response')

    download_to_file(result, h, th_status)
    return True


def check_file(result, th_status):
    log_prefix = result['log_prefix']
    local_path = result['local_path']

    if not os.path.isfile(local_path):
        logger.error('%s the downloaded file is not a file: %s' %
                     (log_prefix, local_path))
        return False

    file_stat = os.stat(local_path)

    local_size = file_stat.st_size
    remote_size = result['file_object']['size']

    if local_size != remote_size:
        logger.error('%s the downloaded file size: %d is not: %d' %
                     (log_prefix, local_size, remote_size))
        return False

    return True


def _download_one_file(result, th_status):
    log_prefix = result['log_prefix']
    key = result['file_object']['key']

    need = check_if_need_download(result, th_status)
    if not need:
        logger.info('%s not need to download file: %s' %
                    (log_prefix, key))
        result['not_need'] = True
        return result

    logger.info('%s start to download file: %s' %
                (log_prefix, key))

    succeed = download_data(result, th_status)
    if not succeed:
        result['download_failed'] = True
        return result

    succeed = check_file(result, th_status)
    if not succeed:
        result['check_failed'] = True
        return result

    logger.info('%s finished to download file: %s' %
                (log_prefix, key))

    return result


def download_one_file(file_object):
    result = {
        'file_object': file_object,
    }

    thread_name = threading.current_thread().getName()
    THREAD_STATUS[thread_name] = THREAD_STATUS.get(thread_name, {})
    th_status = THREAD_STATUS[thread_name]

    try:
        result['log_prefix'] = util.get_log_prefix(file_object['key'])
        result['local_path'] = get_local_path(file_object['s3_key'])

        logger.info('%s about to download file: %s to: %s' %
                    (result['log_prefix'], result['file_object']['key'],
                     result['local_path']))

        th_status['total_n'] = th_status.get('total_n', 0) + 1

        _download_one_file(result, th_status)

        return result

    except Exception as e:
        logger.exception('got exception when process file: %s, %s' %
                         (file_object['key'], repr(e)))
        result['exception'] = True
        th_status['exception'] = th_status.get('exception', 0) + 1

        return result


def update_download_stat(result):
    file_object = result['file_object']
    file_size = file_object['size']

    DOWNLOAD_STATUS['total_n'] += 1
    DOWNLOAD_STATUS['total_size'] += file_size

    if 'exception' in result:
        DOWNLOAD_STATUS['exception_n'] += 1
        DOWNLOAD_STATUS['exception_size'] += file_size

    if 'not_need' in result:
        DOWNLOAD_STATUS['not_need_n'] += 1
        DOWNLOAD_STATUS['not_need_size'] += file_size

    if 'download_failed' in result:
        DOWNLOAD_STATUS['download_failed_n'] += 1
        DOWNLOAD_STATUS['download_failed_size'] += file_size

    if 'check_failed' in result:
        DOWNLOAD_STATUS['check_failed_n'] += 1
        DOWNLOAD_STATUS['check_failed_size'] += file_size


def report_state():
    os.system('clear')
    print '----------------report-----------------'
    print ('speed configuration:(MB)')
    print ', '.join(['%d: %.2f' % (i, speeds[i]) for i in range(24)])

    print ('iter status: total: %d, total size: %.3f (MB), marker: %s' %
           (ITER_STATUS['iter_n'], ITER_STATUS['iter_size'] / 1.0 / MB,
            ITER_STATUS['marker']))

    curr_index = int(time.time()) % len(REAL_SPEED)
    print ('real speeds: %s' %
           ', '.join(['%.3f' %
                      (REAL_SPEED[(i + curr_index + 2) %
                                  len(REAL_SPEED)] / 1.0 / MB)
                      for i in range(len(REAL_SPEED))]))

    print ('download status: total: %d, size: %.3f (MB)' %
           (DOWNLOAD_STATUS['total_n'], DOWNLOAD_STATUS['total_size'] / 1.0 / MB))

    print ('             exception: %d, size: %.3f (MB)' %
           (DOWNLOAD_STATUS['exception_n'], DOWNLOAD_STATUS['exception_size'] / 1.0 / MB))

    print ('              not need: %d, size: %.3f (MB)' %
           (DOWNLOAD_STATUS['not_need_n'], DOWNLOAD_STATUS['not_need_size'] / 1.0 / MB))

    print ('       download failed: %d, size: %.3f (MB)' %
           (DOWNLOAD_STATUS['download_failed_n'],
            DOWNLOAD_STATUS['download_failed_size'] / 1.0 / MB))

    print ('          check failed: %d, size: %.3f (MB)' %
           (DOWNLOAD_STATUS['check_failed_n'],
            DOWNLOAD_STATUS['check_failed_size'] / 1.0 / MB))

    print 'thread status:'

    for k, v in THREAD_STATUS.iteritems():
        print '%s: %s' % (k, repr(v))

    print ''


def report(sess):
    while not sess['stop']:
        report_state()
        time.sleep(cnf['REPORT_INTERVAL'])


def download():
    try:
        sess = {'stop': False}

        report_th = threadutil.start_thread(report, args=(sess,),
                                            daemon=True)

        jobq.run(iter_file(), [(download_one_file, cnf['THREADS_NUM']),
                               (update_download_stat, 1),
                               ])

        sess['stop'] = True

        report_th.join()

        report_state()

    except KeyboardInterrupt:
        report_state()
        sys.exit(0)


def load_cli_args():
    parser = argparse.ArgumentParser(description='dwonload files from s3')
    parser.add_argument('--conf_path', type=str,
                        help='set the path of the conf file')

    args = parser.parse_args()
    return args


def load_conf_from_file(path):
    with open(path) as f:
        conf = yaml.safe_load(f.read())

    return conf


def load_conf(args):
    conf_path = args.conf_path or '../conf/download.yaml'
    conf = load_conf_from_file(conf_path)

    return conf


if __name__ == "__main__":

    cli_args = load_cli_args()
    cnf = load_conf(cli_args)

    tb = token_bucket.TokenBucket(cnf['SPEED'])

    speeds = tb.speed_in_hours

    s3_client = util.get_boto_client(
        cnf['ACCESS_KEY'],
        cnf['SECRET_KEY'],
        endpoint=cnf['ENDPOINT'],
    )

    fsutil.makedirs(cnf['LOG_DIR'])

    log_file_name = 'download-log-for-%s.log' % cnf['BUCKET_NAME']
    logger = util.add_logger(cnf['LOG_DIR'], log_file_name)

    download()
