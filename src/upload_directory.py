#!/usr/bin/env python2
# coding: utf-8

import argparse
import os
import sys
import threading
import time
import urllib
from datetime import datetime

from pykit import awssign
import yaml

import file_filter
import token_bucket
import util
from pykit import http
from pykit import jobq
from pykit import threadutil
from pykit import fsutil

MB = 1024 ** 2

ITER_STATUS = {
    'iter_n': 0,
    'iter_size': 0,
    'marker': 0,
}

UPLOAD_STATUS = {
    'total_n': 0,
    'total_size': 0,
    'encoding_change': 0,
    'exception_n': 0,
    'exception_size': 0,
    'not_need_n': 0,
    'not_need_size': 0,
    'upload_failed_n': 0,
    'upload_failed_size': 0,
    'compare_failed_n': 0,
    'compare_failed_size': 0,
}

THREAD_STATUS = {}


REAL_SPEED = [0] * 10


class DecodeError(Exception):
    pass


def iter_file():
    filter_conf = cnf.get('FILTER_CONF', None)

    try:
        logger.info('start to iter file of: %s' % data_dir)

        for dir in util.iter_dir(data_dir):
            dir_file_list = util.get_dir_file_list(dir)

            for f in dir_file_list:
                f_stat = os.stat(f)
                file_object = {
                    'file_path': f,
                    'size': f_stat.st_size,
                    'last_modified_ts': f_stat.st_mtime,
                    'mime_type': util.get_file_mime_type(f),
                }

                ITER_STATUS['iter_n'] += 1
                ITER_STATUS['iter_size'] += file_object['size']
                ITER_STATUS['marker'] = f

                msg = file_filter.filter(
                    {
                        'key': file_object['file_path'],
                        'size': file_object['size'],
                        'content_type': file_object['mime_type'],
                        'last_modified': datetime.utcfromtimestamp(
                            file_object['last_modified_ts'])
                    }, filter_conf, cnf['TIMEZONE'])

                if msg != None:
                    logger.info('will not upload file: %s, because: %s' %
                                (file_object['file_path'], msg))
                    continue

                yield file_object

            logger.info('iter %d file of: %s' %
                        (len(dir_file_list), dir))

        logger.info('finished to iter file of: %s' % data_dir)

    except Exception as e:
        logger.exception('failed to iter file of: %s, %s' %
                         (data_dir, repr(e)))


def get_s3_file_info(s3_key):
    try:
        unicode_s3_key = util.to_unicode(
            s3_key, cnf.get('ENCODINGS', []))
    except Exception as e:
        logger.warn('failed to decocode: %s' %
                    util.str_to_hex(s3_key))
        raise DecodeError('failed to decode: %s' % repr(e))

    resp = s3_client.head_object(
        Bucket=cnf['BUCKET_NAME'],
        Key=unicode_s3_key,
    )

    s3_file_info = {
        'size': resp['ContentLength'],
        'content_type': resp['ContentType'],
        'meta': resp['Metadata'],
        'content_md5': resp['ETag'].lower().strip('"'),
    }

    return s3_file_info


def get_upload_s3_key(result):
    log_prefix = result['log_prefix']
    file_path = result['file_object']['file_path']

    sub_file_path = file_path[len(cnf['DATA_DIR']):]
    sub_file_path = sub_file_path.lstrip('/')

    sub_file_path_utf8_parts = []
    for part in sub_file_path.split('/'):
        sub_file_path_utf8_parts.append(
            util.to_utf8(part, encodings=cnf.get('ENCODINGS', [])))

    sub_file_path_utf8 = '/'.join(sub_file_path_utf8_parts)

    sub_file_path_hex = util.str_to_hex(sub_file_path)
    sub_file_path_utf8_hex = util.str_to_hex(sub_file_path_utf8)

    s3_key = sub_file_path

    if sub_file_path_hex != sub_file_path_utf8_hex:
        logger.warn('%s file path is not utf8 encode: %s vs %s' %
                    (log_prefix, sub_file_path_hex, sub_file_path_utf8_hex))

        if cnf['UTF8_ENCODE_KEY']:
            logger.info('%s change file path: %s to utf8' %
                        (log_prefix, file_path))
            s3_key = sub_file_path_utf8
            UPLOAD_STATUS['encoding_change'] += 1

    if len(cnf['KEY_PREFIX']) > 1:
        s3_key = cnf['KEY_PREFIX'] + '/' + s3_key

    return s3_key


def check_if_need_upload(result, th_status):
    if not cnf['CHECK_EXIST']:
        return True

    log_prefix = result['log_prefix']

    file_object = result['file_object']
    s3_key = result['s3_key']

    try:
        s3_file_info = get_s3_file_info(s3_key)

    except DecodeError as e:
        return True

    except Exception as e:
        if hasattr(e, 'message') and 'Not Found' in e.message:
            logger.info('%s file: %s not found in s3, need to upload' %
                        (log_prefix, s3_key))
            return True
        else:
            logger.exception(('%s faied to get s3 file info when check ' +
                              'if need to upload %s: %s') %
                             (log_prefix, s3_key, repr(e)))
            th_status['s3_get_error'] = th_status.get('s3_get_error', 0) + 1
            return False

    th_status['exist'] = th_status.get('exist', 0) + 1

    if s3_file_info['size'] != file_object['size']:
        th_status['size_not_equal'] = th_status.get('size_not_equal', 0) + 1
        logger.info(('%s need to override file: %s, because size not equal, ' +
                     'local_size: %d, s3_size: %d') %
                    (log_prefix, s3_key, file_object['size'], s3_file_info['size']))
        return True

    if cnf['OVERRIDE']:
        th_status['override'] = th_status.get('override', 0) + 1
        return True
    else:
        return False


def _upload_data(result, th_status):
    file_object = result['file_object']
    s3_key = result['s3_key']

    log_prefix = result['log_prefix']
    file_path = file_object['file_path']

    s3_request = {
        'verb': 'PUT',
        'uri': '/' + cnf['BUCKET_NAME'] + '/' + urllib.quote(s3_key),
        'headers': {
            'Host': cnf['ENDPOINT'],
            'Content-Length': file_object['size'],
            'Content-Type': file_object['mime_type'],
            'X-Amz-Acl': cnf['FILE_ACL'],
        },
    }

    s3_signer.add_auth(s3_request, sign_payload=False)

    http_s3 = http.Client(cnf['ENDPOINT'], 80, timeout=60 * 60)

    http_s3.send_request(s3_request['uri'],
                         method=s3_request['verb'],
                         headers=s3_request['headers'])

    f = open(file_path, 'rb')

    file_size = file_object['size']
    uploaded_size = 0
    while True:
        tokens = min(file_size - uploaded_size, cnf['UPLOAD_CHUNK_SIZE'])
        tb.get_tokens(tokens)

        curr_second = int(time.time())
        index = curr_second % len(REAL_SPEED)
        next_index = (curr_second + 1) % len(REAL_SPEED)

        REAL_SPEED[next_index] = 0
        REAL_SPEED[index] += tokens

        buf = f.read(tokens)

        logger.info('%s read %d bytes from file: %s' %
                    (log_prefix, len(buf), file_path))

        if len(buf) != tokens:
            logger.error(('%s failed to read file: %s,' +
                          ' tokens: %d, data length: %d') %
                         (log_prefix, file_path, tokens, len(buf)))
            th_status['read_file_error'] = th_status.get(
                'read_file_error', 0) + 1
            return False

        http_s3.send_body(buf)

        logger.info('%s uploaded %d bytes for file: %s' %
                    (log_prefix, len(buf), file_path))

        uploaded_size += len(buf)

        th_status['progress'] = (
            uploaded_size, file_size, (uploaded_size + 1.0) / (file_size + 1))

        if uploaded_size == file_size:
            break

    http_s3.read_response()

    if http_s3.status != 200:
        logger.error(('%s got invalid response from s3 when upload file to: %s,' +
                      'status: %d, resp: %s') %
                     (log_prefix, s3_key, http_s3.status, http_s3.read_body(1024)))
        th_status['upload_s3_error'] = th_status.get('upload_s3_error', 0) + 1
        return False

    return True


def upload_data(result, th_status):
    log_prefix = result['log_prefix']
    file_path = result['file_object']['file_path']

    try:
        succeed = _upload_data(result, th_status)

        if not succeed:
            result['pipe_failed'] = True
            return False

        return True

    except Exception as e:
        logger.exception('%s got exception when upload file %s: %s' %
                         (log_prefix, file_path, repr(e)))

        result['upload_failed'] = True
        th_status['upload_exception'] = th_status.get(
            'upload_exception', 0) + 1

        return False


def compare_file_info(result, s3_file_info, th_status):
    log_prefix = result['log_prefix']
    file_object = result['file_object']
    file_path = file_object['file_path']

    if file_object['size'] != s3_file_info['size']:
        logger.error(('%s compare failed for file: %s, local file size: %d, ' +
                      's3 file size: %d') %
                     (log_prefix, file_path, file_object['size'],
                      s3_file_info['size']))

        th_status['compare_size_error'] = th_status.get(
            'compare_size_error', 0) + 1
        return False

    if file_object['mime_type'].lower() != s3_file_info['content_type'].lower():
        logger.error(('%s compare failed for file: %s, local content type: %s, ' +
                      's3 content type: %s') %
                     (log_prefix, file_path, file_object['mime_type'],
                      s3_file_info['content_type']))

        th_status['compare_type_error'] = th_status.get(
            'compare_type_error', 0) + 1
        return False

    return True


def compare_file(result, th_status):
    log_prefix = result['log_prefix']
    s3_key = result['s3_key']

    try:
        s3_file_info = get_s3_file_info(s3_key)

    except Exception as e:
        th_status['compare_s3_error'] = th_status.get(
            'compare_s3_error', 0) + 1

        logger.exception('%s got exception when get s3 file info %s: %s' %
                         (log_prefix, s3_key, repr(e)))
        return False

    succeed = compare_file_info(result, s3_file_info, th_status)
    if not succeed:
        return False

    return True


def _upload_one_file(result, th_status):
    log_prefix = result['log_prefix']
    file_path = result['file_object']['file_path']

    need = check_if_need_upload(result, th_status)
    if not need:
        logger.info('%s not need to upload file: %s' %
                    (log_prefix, file_path))
        result['not_need'] = True
        return result

    logger.info('%s start to upload file: %s' %
                (log_prefix, file_path))

    succeed = upload_data(result, th_status)
    if not succeed:
        result['download_failed'] = True
        return result

    succeed = compare_file(result, th_status)
    if not succeed:
        result['check_failed'] = True
        return result

    logger.info('%s finished to upload file: %s' %
                (log_prefix, file_path))

    return result


def upload_one_file(file_object):
    result = {
        'file_object': file_object,
    }

    thread_name = threading.current_thread().getName()
    THREAD_STATUS[thread_name] = THREAD_STATUS.get(thread_name, {})
    th_status = THREAD_STATUS[thread_name]

    try:
        result['log_prefix'] = util.get_log_prefix(file_object['file_path'])

        result['s3_key'] = get_upload_s3_key(result)

        logger.info('%s about to upload file: %s to: %s' %
                    (result['log_prefix'], file_object['file_path'],
                     result['s3_key']))

        th_status['total_n'] = th_status.get('total_n', 0) + 1

        _upload_one_file(result, th_status)

        if cnf['CLEAR_FILES']:
            fsutil.remove(file_object['file_path'])

        return result

    except Exception as e:
        logger.exception('got exception when process file: %s, %s' %
                         (file_object['file_path'], repr(e)))
        result['exception'] = True
        th_status['exception'] = th_status.get('exception', 0) + 1

        return result


def upload_directory():
    try:
        sess = {'stop': False}

        report_th = threadutil.start_thread(report, args=(sess,),
                                            daemon=True)

        jobq.run(iter_file(), [(upload_one_file, cnf['THREADS_NUM']),
                               (update_upload_stat, 1),
                               ])

        sess['stop'] = True
        report_th.join()

        report_state()

    except KeyboardInterrupt:
        report_state()
        sys.exit(0)


def update_upload_stat(result):
    file_object = result['file_object']
    file_size = file_object['size']

    UPLOAD_STATUS['total_n'] += 1
    UPLOAD_STATUS['total_size'] += file_size

    if 'exception' in result:
        UPLOAD_STATUS['exception_n'] += 1
        UPLOAD_STATUS['exception_size'] += file_size

    if 'not_need' in result:
        UPLOAD_STATUS['not_need_n'] += 1
        UPLOAD_STATUS['not_need_size'] += file_size

    if 'pipe_failed' in result:
        UPLOAD_STATUS['upload_failed_n'] += 1
        UPLOAD_STATUS['upload_failed_size'] += file_size

    if 'compare_failed' in result:
        UPLOAD_STATUS['compare_failed_n'] += 1
        UPLOAD_STATUS['compare_failed_size'] += file_size


def report_state():
    os.system('clear')
    print '----------------report-----------------'
    print ('speed configuration:(MB)')
    print ', '.join(['%d: %.1f' % (i, speeds[i]) for i in range(24)])

    print ('iter status: total: %d, total size: %.3f (MB), marker: %s' %
           (ITER_STATUS['iter_n'], ITER_STATUS['iter_size'] / 1.0 / MB,
            ITER_STATUS['marker']))

    curr_index = int(time.time()) % len(REAL_SPEED)
    print ('real speeds: %s' %
           ', '.join(['%.3f' %
                      (REAL_SPEED[(i + curr_index + 2) %
                                  len(REAL_SPEED)] / 1.0 / MB)
                      for i in range(len(REAL_SPEED))]))

    print ('upload status:  total: %d, size: %.3f (MB), encoding_change: %d' %
           (UPLOAD_STATUS['total_n'], UPLOAD_STATUS['total_size'] / 1.0 / MB,
            UPLOAD_STATUS['encoding_change']))

    print ('            exception: %d, size: %.3f (MB)' %
           (UPLOAD_STATUS['exception_n'], UPLOAD_STATUS['exception_size'] / 1.0 / MB))

    print ('             not need: %d, size: %.3f (MB)' %
           (UPLOAD_STATUS['not_need_n'], UPLOAD_STATUS['not_need_size'] / 1.0 / MB))

    print ('        upload_failed: %d, size: %.3f (MB)' %
           (UPLOAD_STATUS['upload_failed_n'],
            UPLOAD_STATUS['upload_failed_size'] / 1.0 / MB))

    print ('       compare failed: %d, size: %.3f (MB)' %
           (UPLOAD_STATUS['compare_failed_n'],
            UPLOAD_STATUS['compare_failed_size'] / 1.0 / MB))

    print 'thread status:'

    for k, v in THREAD_STATUS.iteritems():
        print '%s: %s' % (k, repr(v))

    print ''


def report(sess):
    while not sess['stop']:
        report_state()
        time.sleep(cnf['REPORT_INTERVAL'])


def load_cli_args():
    parser = argparse.ArgumentParser(
        description='upload local directory to s3')
    parser.add_argument('--conf_path', type=str,
                        help='set the path of the conf file')

    args = parser.parse_args()
    return args


def load_conf_from_file(path):
    with open(path) as f:
        conf = yaml.safe_load(f.read())

    return conf


def load_conf(args):
    conf_path = args.conf_path or '../conf/upload_directory.yaml'
    conf = load_conf_from_file(conf_path)

    return conf


if __name__ == "__main__":

    cli_args = load_cli_args()
    cnf = load_conf(cli_args)

    tb = token_bucket.TokenBucket(cnf['SPEED'])

    data_dir = cnf['DATA_DIR']
    if not data_dir.startswith('/'):
        print 'the directory name is not absolute path: ' + data_dir
        sys.exit()

    if not os.path.exists(data_dir) or not os.path.isdir(data_dir):
        print data_dir + ' is not exists or is not a directory'
        sys.exit()

    speeds = tb.speed_in_hours

    s3_client = util.get_boto_client(
        cnf['ACCESS_KEY'],
        cnf['SECRET_KEY'],
        endpoint=cnf['ENDPOINT'],
    )

    s3_signer = awssign.Signer(cnf['ACCESS_KEY'],
                               cnf['SECRET_KEY'])

    fsutil.makedirs(cnf['LOG_DIR'])

    log_file_name = ('upload-log-for-%s.log' %
                     cnf['DATA_DIR'].replace('/', '_'))
    logger = util.add_logger(cnf['LOG_DIR'], log_file_name)

    upload_directory()
