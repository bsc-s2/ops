#!/usr/bin/env python2
# coding:utf-8

import argparse
import errno
import logging
import os
import sys
import threading
import time
import urllib
from urlparse import urlparse

import boto3
import yaml
from botocore.client import Config

import file_filter
from pykit import awssign
import base
import token_bucket
import uploader
import util
from pykit import http
from pykit import fsutil
from pykit import jobq


MB = 1024 ** 2

TOKEN_LOCK = threading.RLock()

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
    'not_need_n': 0,
    'not_need_size': 0,
    'pipe_failed_n': 0,
    'pipe_failed_size': 0,
    'compare_failed_n': 0,
    'compare_failed_size': 0,
}

AWS_META_PREFIX = 'x-amz-meta-'
S3_META_PREFIX = 'x-amz-meta-'
CHUNK_SIZE = 10240


REAL_SPEED = [0] * 10


class DownloadFromAwsError(Exception):
    pass


class ExtractFileInfoError(Exception):
    pass


def add_logger():

    log_file = os.path.join(cnf['LOG_DIR'], 'aws-sync-for-' +
                            cnf['AWS_BUCKET_NAME'] + '.log')

    log = logging.getLogger()
    log.setLevel(logging.INFO)

    file_handler = logging.FileHandler(log_file)
    formatter = logging.Formatter('[%(asctime)s, %(levelname)s] %(message)s')

    file_handler.setFormatter(formatter)

    log.addHandler(file_handler)

    return log


def _mkdir(path):
    try:
        os.makedirs(path, 0755)
    except OSError as e:
        if e[0] == errno.EEXIST and os.path.isdir(path):
            pass
        else:
            raise


def _thread(func, args):
    th = threading.Thread(target=func, args=args)
    th.daemon = True
    th.start()

    return th


def get_boto_client(endpoint):
    client = boto3.client(
        's3',
        use_ssl=False,
        aws_access_key_id=cnf['BAISHAN_ACCESS_KEY'],
        aws_secret_access_key=cnf['BAISHAN_SECRET_KEY'],
        config=Config(signature_version='s3v4'),
        region_name='us-east-1',
        endpoint_url=endpoint,
    )

    return client


def load_progress():
    return {
        'total_n': 0,
        'total_size': 0,
        'marker': '',
    }


def iter_files():
    marker = cnf.get('START_MARKER', '')
    end_marker = cnf.get('END_MARKER', None)
    filter_conf = cnf.get('FILTER_CONF', None)

    try:
        while True:
            resp = aws_client.list_objects(
                Bucket=cnf['AWS_BUCKET_NAME'],
                Marker=marker,
                Prefix=cnf['PREFIX'],
            )

            if 'Contents' not in resp:
                print '### iter file end at: ' + ITER_STATUS['marker']
                return

            for content in resp['Contents']:
                file_object = {
                    'key': content['Key'],
                    'etag': content['ETag'].lower().strip('"'),
                    'last_modified': content['LastModified'],
                    'size': content['Size'],
                }
                if end_marker is not None and content['Key'] >= end_marker:
                    print '### iter file end marker reached'
                    return

                ITER_STATUS['iter_n'] += 1
                ITER_STATUS['iter_size'] += content['Size']
                ITER_STATUS['marker'] = content['Key']

                msg = file_filter.filter(file_object, filter_conf, cnf['TIMEZONE'])
                if msg != None:
                    logger.info('will not sync file: %s, because: %s' %
                                (file_object['key'], msg))
                    continue

                yield file_object

                marker = content['Key']

    except Exception as e:
        logger.exception('failed to iter file: ' + repr(e))
        print 'failed to iter file: ' + repr(e)


def get_aws_user_meta(headers):
    meta = {}
    for k, v in headers.iteritems():
        if k.lower().startswith(AWS_META_PREFIX):
            meta_name = k.lower()[len(AWS_META_PREFIX):]
            meta[meta_name] = v

    return meta


def validate_and_extract_aws_file_info(headers, result, th_status):
    file_object = result['file_object']
    if not 'content-length' in headers:
        th_status['aws_no_length'] = th_status.get('aws_no_length', 0) + 1
        return

    content_length = int(headers['content-length'])
    if content_length != file_object['size']:
        logger.error(('aws response content length: %d, is not equal ' +
                      'to file size: %d') %
                     (content_length, file_object['size']))
        th_status['aws_length_error'] = th_status.get(
            'aws_length_error', 0) + 1
        return

    if not 'content-type' in headers:
        th_status['aws_no_type'] = th_status.get('aws_no_type', 0) + 1
        return

    aws_file_info = {
        'key': file_object['key'],
        'size': file_object['size'],
        'content_type': headers['content-type'],
    }

    if 'etag' in headers:
        etag = headers['etag'].lower().strip('"')

        if etag != file_object['etag']:
            logger.error(('aws response etag: %s, is not equal to ' +
                          'file etag: %s') %
                         (etag, file_object['etag']))
            th_status['aws_etag_error'] = th_status.get('aws_etag_error', 0) + 1
            return

        aws_file_info['etag'] = etag

    else:
        th_status['aws_no_etag'] = th_status.get('aws_no_etag', 0) + 1

    aws_file_info['meta'] = get_aws_user_meta(headers)

    return aws_file_info


def download_from_aws(signed_object_url):
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


def pipe_file(result, http_aws, th_status):
    log_prefix = result['log_prefix']
    key = result['file_object']['key']

    aws_file_info = result['aws_file_info']
    file_size = aws_file_info['size']

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
        for k, v in aws_file_info['meta'].iteritems():
            header_name = S3_META_PREFIX + k
            extra_headers[header_name] = v

        uploader.s3_upload(http_read=http_aws,
                           s3_key=urllib.quote(result['s3_key']),
                           file_size=file_size,
                           content_type=aws_file_info['content_type'],
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


def convert_key(key):
    return key.encode(encoding='utf-8')


def _sync_one_file(result, th_status):
    log_prefix = result['log_prefix']
    aws_key = result['file_object']['key']

    signed_object_url = aws_client.generate_presigned_url(
        'get_object',
        Params={
            'Bucket': cnf['AWS_BUCKET_NAME'],
            'Key': aws_key,
        },
        ExpiresIn=60 * 60
    )

    status, headers, http_aws = download_from_aws(signed_object_url)

    if status != 200:
        body = http_aws.read_body(1024)
        message = (('got invalid response when doload file: %s ' +
                    'from aws, %s, %s, %s') %
                   (aws_key, repr(status), repr(headers), body))
        th_status['aws_get_error'] = th_status.get('aws_get_error', 0) + 1
        raise DownloadFromAwsError(message)

    aws_file_info = validate_and_extract_aws_file_info(
        headers, result, th_status)
    if aws_file_info == None:
        result['pipe_failed'] = True
        raise ExtractFileInfoError('invalid aws response')

    result['aws_file_info'] = aws_file_info

    need = base.check_if_need_sync(
        conf=cnf, file_info=aws_file_info, bucket_name=cnf['BAISHAN_BUCKET_NAME'],
        s3_key=result['s3_key'], s3_client=baishan_client, log_prefix=log_prefix,
        status=th_status)

    if not need:
        result['not_need'] = True
        logger.info('%s do not need to sync file: %s' %
                    (log_prefix, aws_key))
        return result

    succeed = pipe_file(result, http_aws, th_status)
    if not succeed:
        result['pipe_failed'] = True
        return result

    succeed = base.compare_file(
        bucket_name=cnf['BAISHAN_BUCKET_NAME'],
        s3_key=result['s3_key'],
        file_info=aws_file_info,
        s3_client=baishan_client, log_prefix=log_prefix, status=th_status)

    if not succeed:
        result['compare_failed'] = True
        return result

    logger.info('%s finished to sync file: %s' %
                (log_prefix, aws_key))

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

        result['s3_key'] = convert_key(file_object['key'])

        th_status['sync_n'] = th_status.get('sync_n', 0) + 1

        _sync_one_file(result, th_status)

        return result

    except Exception as e:
        logger.exception('got exception when sync one file %s: %s' %
                         (repr(e), repr(e)))
        result['pipe_failed'] = True
        th_status['pipe_exception'] = th_status.get('pipe_exception', 0) + 1

        return result


def update_sync_stat(result):
    file_object = result['file_object']
    file_size = file_object['size']

    SYNC_STATUS['total_n'] += 1
    SYNC_STATUS['total_size'] += file_size

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

    print ('sync status:    total: %d, size: %.3f (MB), speed: %.3f (MB)' %
           (SYNC_STATUS['total_n'], SYNC_STATUS['total_size'] / 1.0 / MB,
            SYNC_STATUS['speed'] / 1.0 / MB))

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

        report_th = _thread(report, (sess,))

        jobq.run(iter_files(), [(sync_one_file, cnf['THREADS_NUM']),
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
    conf_path = args.conf_path or '../conf/aws_sync.yaml'
    conf = load_conf_from_file(conf_path)

    return conf


if __name__ == "__main__":

    cli_args = load_cli_args()
    cnf = load_conf(cli_args)

    tb = token_bucket.TokenBucket(cnf['SPEED'])

    speeds = tb.speed_in_hours

    baishan_client = boto3.client(
        's3',
        use_ssl=False,
        aws_access_key_id=cnf['BAISHAN_ACCESS_KEY'],
        aws_secret_access_key=cnf['BAISHAN_SECRET_KEY'],
        config=Config(signature_version='s3v4'),
        region_name='us-east-1',
        endpoint_url='http://' + cnf['BAISHAN_ENDPOINT'],
    )

    aws_client = boto3.client(
        's3',
        use_ssl=False,
        aws_access_key_id=cnf['AWS_ACCESS_KEY'],
        aws_secret_access_key=cnf['AWS_SECRET_KEY'],
        config=Config(signature_version='s3v4'),
        region_name=cnf['AWS_REGION'],
        endpoint_url='http://' + cnf['AWS_ENDPOINT'],
    )

    s3_signer = awssign.Signer(cnf['BAISHAN_ACCESS_KEY'],
                               cnf['BAISHAN_SECRET_KEY'])

    _mkdir(cnf['LOG_DIR'])
    fsutil.makedirs(cnf['LOG_DIR'])

    log_file_name = 'aws-sync-log-for-%s.log' % cnf['AWS_BUCKET_NAME']
    logger = util.add_logger(cnf['LOG_DIR'], log_file_name)


    sync()
