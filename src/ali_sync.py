#!/usr/bin/env python2
# coding:utf-8

import argparse
import base64
import os
import sys
import threading
import time
import urllib
from datetime import datetime
from urlparse import urlparse

import oss2
import yaml

from pykit import awssign
import base
import file_filter
import token_bucket
import uploader
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


SYNC_STATUS = {
    'speed': 0,
    'total_n': 0,
    'total_size': 0,
    'encoding_change': 0,
    'exception_n': 0,
    'exception_size': 0,
    'not_need_n': 0,
    'not_need_size': 0,
    'pipe_failed_n': 0,
    'pipe_failed_size': 0,
    'compare_failed_n': 0,
    'compare_failed_size': 0,
}

ALI_META_PREFIX = 'x-oss-meta-'
S3_META_PREFIX = 'x-amz-meta-'


REAL_SPEED = [0] * 10


class DownloadFromAliError(Exception):
    pass


class ExtractFileInfoError(Exception):
    pass


def iter_file():
    prefix = cnf.get('PREFIX', '')
    marker = cnf.get('START_MARKER', '')
    end_marker = cnf.get('END_MARKER', None)
    filter_conf = cnf.get('FILTER_CONF', None)

    logger.info('start to iter file from: ' + repr(marker))

    try:
        for file_object_oss in oss2.ObjectIterator(oss2_bucket,
                                                   prefix=prefix,
                                                   marker=marker):
            file_object = {
                'ali_key': file_object_oss.key,
                'key': util.to_utf8(file_object_oss.key,
                                    encodings=cnf.get('ENCODINGS', [])),
                'last_modified': datetime.utcfromtimestamp(
                    file_object_oss.last_modified),
                'size': file_object_oss.size,
                'etag': file_object_oss.etag,
            }

            if end_marker and file_object['key'] > end_marker:
                logger.info('iter file end, end marker: %s reached' %
                            end_marker)
                break

            ITER_STATUS['iter_n'] += 1
            ITER_STATUS['iter_size'] += file_object['size']
            ITER_STATUS['marker'] = file_object['key']

            msg = file_filter.filter(file_object, filter_conf, cnf['TIMEZONE'])
            if msg != None:
                logger.info('will not sync file: %s, because: %s' %
                            (file_object['key'], msg))
                continue

            yield file_object

        logger.info('iter file end at: %s' % repr(ITER_STATUS['marker']))

    except Exception as e:
        logger.exception('failed to list file: ' + repr(e))


def get_ali_user_meta(headers):
    meta = {}
    for k, v in headers.iteritems():
        if k.lower().startswith(ALI_META_PREFIX):
            meta_name = k.lower()[len(ALI_META_PREFIX):]
            meta[meta_name] = v

    return meta


def validate_and_extract_ali_file_info(headers, result, th_status):
    log_prefix = result['log_prefix']
    file_object = result['file_object']

    if not 'content-length' in headers:
        logger.error('%s ali response has no content length' % log_prefix)
        th_status['ali_no_length'] = th_status.get('ali_no_length', 0) + 1
        return

    content_length = int(headers['content-length'])
    if content_length != file_object['size']:
        logger.error(('%s ali response content length: %d, is not equal ' +
                      'to file size: %d') %
                     (log_prefix, content_length, file_object['size']))
        th_status['ali_length_error'] = th_status.get(
            'ali_length_error', 0) + 1
        return

    if not 'content-type' in headers:
        logger.error('%s ali response has no content type' % log_prefix)
        th_status['ali_no_type'] = th_status.get('ali_no_type', 0) + 1
        return

    ali_file_info = {
        'key': file_object['key'],
        'size': file_object['size'],
        'content_type': headers['content-type'],
    }

    if 'content-md5' in headers:
        md5 = base64.b64decode(headers['content-md5'])
        md5 = md5.encode('hex')

        if md5 != file_object['etag'].lower():
            logger.error(('%s ali response content md5: %s, is not equal to ' +
                          'file etag: %s') %
                         (log_prefix, md5, file_object['etag']))
            th_status['ali_md5_error'] = th_status.get('ali_md5_error', 0) + 1
            return

        ali_file_info['md5'] = md5

    else:
        th_status['ali_no_md5'] = th_status.get('ali_no_md5', 0) + 1

    ali_file_info['meta'] = get_ali_user_meta(headers)

    return ali_file_info


def download_from_ali(signed_object_url):
    parse_result = urlparse(signed_object_url)
    host, _, port = parse_result.netloc.partition(':')
    if len(port) > 0:
        port = int(port)
    else:
        port = 80
    uri = parse_result.path
    if len(parse_result.query) > 0:
        uri += '?' + parse_result.query

    h = http.Client(host, port, timeout=60 * 60)
    h.send_request(uri)
    h.read_response()

    return h.status, h.headers, h


def pipe_file(result, http_ali, th_status):
    log_prefix = result['log_prefix']
    key = result['file_object']['key']

    ali_file_info = result['ali_file_info']
    file_size = ali_file_info['size']

    def callback(progress):
        piped_size = progress['piped_size']
        th_status['progress'] = (
            piped_size, file_size, (piped_size + 1.0) / (file_size + 1))

        curr_second = int(time.time())
        index = curr_second % len(REAL_SPEED)
        next_index = (curr_second + 1) % len(REAL_SPEED)

        REAL_SPEED[next_index] = 0
        REAL_SPEED[index] += progress['tokens']

        logger.info('%s read %d bytes from ali for file: %s' %
                    (log_prefix, progress['tokens'], key))

    try:
        extra_headers = {}
        for k, v in ali_file_info['meta'].iteritems():
            header_name = S3_META_PREFIX + k
            extra_headers[header_name] = v

        uploader.s3_upload(http_read=http_ali,
                           s3_key=urllib.quote(result['s3_key']),
                           file_size=file_size,
                           content_type=ali_file_info['content_type'],
                           token_bucket=tb,
                           s3_signer=s3_signer,
                           bucket_name=cnf['BAISHAN_BUCKET_NAME'],
                           endpoint=cnf['BAISHAN_ENDPOINT'],
                           multipart_threshold=cnf['MULTIPART_THRESHOLD'],
                           extra_headers=extra_headers,
                           callback=callback)
    except Exception as e:
        logger.exception('%s failed to pipe %s: %s' %
                         (log_prefix, key, repr(e)))
        result['pipe_failed'] = True
        return False

    return True


def convert_key(result):
    ali_key = result['file_object']['ali_key']
    utf8_key = result['file_object']['key']

    ali_key_hex = util.str_to_hex(ali_key)
    utf8_key_hex = util.str_to_hex(utf8_key)

    s3_key = ali_key

    if ali_key_hex != utf8_key_hex:
        logger.info('%s ali key not utf8 encode, %s vs %s' %
                    (result['log_prefix'], ali_key_hex, utf8_key_hex))
        if cnf['UTF8_ENCODE_KEY']:
            SYNC_STATUS['encoding_change'] += 1
            s3_key = utf8_key

    if isinstance(s3_key, unicode):
        logger.warn('%s ali key: %s is unicode, change to utf8' %
                    (result['log_prefix'], util.str_to_hex(s3_key)))
        s3_key = s3_key.encode('utf-8')

    return s3_key


def _sync_one_file(result, th_status):
    log_prefix = result['log_prefix']
    key = result['file_object']['key']

    ali_key = result['file_object']['ali_key']
    signed_object_url = oss2_bucket.sign_url('GET', ali_key, 60)

    status, headers, http_ali = download_from_ali(signed_object_url)

    if status != 200:
        body = http_ali.read_body(1024)
        message = (('%s got invalid response when doload file: %s ' +
                    'from ali, %s, %s, %s') %
                   (log_prefix, key, repr(status), repr(headers), body))
        th_status['ali_get_error'] = th_status.get('ali_get_error', 0) + 1
        raise DownloadFromAliError(message)

    ali_file_info = validate_and_extract_ali_file_info(
        headers, result, th_status)
    if ali_file_info == None:
        result['pipe_failed'] = True
        raise ExtractFileInfoError('invalid ali response')

    result['ali_file_info'] = ali_file_info

    need = base.check_if_need_sync(
        conf=cnf, file_info=ali_file_info, bucket_name=cnf['BAISHAN_BUCKET_NAME'],
        s3_key=result['s3_key'], s3_client=s3_client, log_prefix=log_prefix,
        status=th_status)

    if not need:
        result['not_need'] = True
        logger.info('%s do not need to sync file: %s' %
                    (log_prefix, key))
        return result

    succeed = pipe_file(result, http_ali, th_status)
    if not succeed:
        result['pipe_failed'] = True
        return result

    succeed = base.compare_file(
        bucket_name=cnf['BAISHAN_BUCKET_NAME'],
        s3_key=result['s3_key'],
        file_info=ali_file_info,
        s3_client=s3_client, log_prefix=log_prefix, status=th_status)

    if not succeed:
        result['compare_failed'] = True
        return result

    logger.info('%s finished to sync file: %s' %
                (log_prefix, key))

    return result


def sync_one_file(file_object):
    result = {
        'file_object': file_object,
    }
    thread_name = threading.current_thread().getName()
    THREAD_STATUS[thread_name] = THREAD_STATUS.get(thread_name, {})
    th_status = THREAD_STATUS[thread_name]

    try:
        result['log_prefix'] = util.get_log_prefix(file_object['key'])

        logger.info('%s about to sync file: %s' %
                    (result['log_prefix'], file_object['key']))

        result['s3_key'] = convert_key(result)

        th_status['sync_n'] = th_status.get('sync_n', 0) + 1

        _sync_one_file(result, th_status)

        return result

    except Exception as e:
        logger.exception('got exception when sync one file %s: %s' %
                         (file_object['key'], repr(e)))
        result['exception'] = True
        th_status['exception'] = th_status.get('exception', 0) + 1

        return result


def update_sync_stat(result):
    file_object = result['file_object']
    file_size = file_object['size']

    SYNC_STATUS['total_n'] += 1
    SYNC_STATUS['total_size'] += file_size

    if 'exception' in result:
        SYNC_STATUS['exception_n'] += 1
        SYNC_STATUS['exception_size'] += file_size

    if 'not_need' in result:
        SYNC_STATUS['not_need_n'] += 1
        SYNC_STATUS['not_need_size'] += file_size

    if 'pipe_failed' in result:
        SYNC_STATUS['pipe_failed_n'] += 1
        SYNC_STATUS['pipe_failed_size'] += file_size

    if 'compare_failed' in result:
        SYNC_STATUS['compare_failed_n'] += 1
        SYNC_STATUS['compare_failed_size'] += file_size


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

    print ('sync status:    total: %d, size: %.3f (MB), encoding_change: %d' %
           (SYNC_STATUS['total_n'], SYNC_STATUS['total_size'] / 1.0 / MB,
            SYNC_STATUS['encoding_change']))

    print ('            exception: %d, size: %.3f (MB)' %
           (SYNC_STATUS['exception_n'], SYNC_STATUS['exception_size'] / 1.0 / MB))

    print ('             not need: %d, size: %.3f (MB)' %
           (SYNC_STATUS['not_need_n'], SYNC_STATUS['not_need_size'] / 1.0 / MB))

    print ('          pipe failed: %d, size: %.3f (MB)' %
           (SYNC_STATUS['pipe_failed_n'],
            SYNC_STATUS['pipe_failed_size'] / 1.0 / MB))

    print ('       compare failed: %d, size: %.3f (MB)' %
           (SYNC_STATUS['compare_failed_n'],
            SYNC_STATUS['compare_failed_size'] / 1.0 / MB))

    print 'thread status:'

    for k, v in THREAD_STATUS.iteritems():
        print '%s: %s' % (k, repr(v))

    print ''


def report(sess):
    while not sess['stop']:
        report_state()
        time.sleep(cnf['REPORT_INTERVAL'])


def sync():
    try:
        sess = {'stop': False}

        report_th = threadutil.start_thread(report, args=(sess,),
                                            daemon=True)

        jobq.run(iter_file(), [(sync_one_file, cnf['THREADS_NUM']),
                               (update_sync_stat, 1),
                               ])

        sess['stop'] = True
        report_th.join()

        report_state()

    except KeyboardInterrupt:
        report_state()
        sys.exit(0)


def load_cli_args():
    parser = argparse.ArgumentParser(description='transfer files')
    parser.add_argument('--conf_path', type=str,
                        help='set the path of the conf file')

    args = parser.parse_args()
    return args


def load_conf_from_file(path):
    with open(path) as f:
        conf = yaml.safe_load(f.read())

    return conf


def load_conf(args):
    conf_path = args.conf_path or '../conf/ali_sync.yaml'
    conf = load_conf_from_file(conf_path)

    return conf


if __name__ == "__main__":

    cli_args = load_cli_args()
    cnf = load_conf(cli_args)

    tb = token_bucket.TokenBucket(cnf['SPEED'])

    speeds = tb.speed_in_hours

    oss2_auth = oss2.Auth(cnf['ALI_ACCESS_KEY'], cnf['ALI_SECRET_KEY'])
    oss2_bucket = oss2.Bucket(
        oss2_auth, cnf['ALI_ENDPOINT'], cnf['ALI_BUCKET_NAME'])

    s3_client = util.get_boto_client(
        cnf['BAISHAN_ACCESS_KEY'],
        cnf['BAISHAN_SECRET_KEY'],
        endpoint=cnf['BAISHAN_ENDPOINT'],
    )

    s3_signer = awssign.Signer(cnf['BAISHAN_ACCESS_KEY'],
                               cnf['BAISHAN_SECRET_KEY'])

    fsutil.makedirs(cnf['LOG_DIR'])

    log_file_name = 'ali-sync-log-for-%s.log' % cnf['ALI_BUCKET_NAME']
    logger = util.add_logger(cnf['LOG_DIR'], log_file_name)

    sync()
