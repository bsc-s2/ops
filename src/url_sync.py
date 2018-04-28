#!/usr/bin/env python2
# coding:utf-8

import argparse
import os
import sys
import threading
import time
from urlparse import urlparse

import yaml

from pykit import awssign
import base
import token_bucket
import uploader
import util
from pykit import http
from pykit import fsutil
from pykit import jobq
from pykit import threadutil

MB = 1024 ** 2


class DownloadFromUrlError(Exception):
    pass


class ExtractFileInfoError(Exception):
    pass


THREAD_STATUS = {}

ITER_STATUS = {
    'iter_n': 0,
    'marker': '',
}


SYNC_STATUS = {
    'speed': 0,
    'total_n': 0,
    'total_size': 0,
    'exception_n': 0,
    'exception_size': 0,
    'not_need_n': 0,
    'pipe_failed_n': 0,
    'compare_failed_n': 0,
}


REAL_SPEED = [0] * 10


def iter_url():
    with open(cnf['URL_LIST_FILE'], 'r') as f:
        lines = f.readlines()

    for line in lines:
        ITER_STATUS['iter_n'] += 1
        ITER_STATUS['marker'] = line

        line = line.strip()
        parts = line.split()

        yield (parts[0], parts[1])


def extract_url_file_info(headers, result, th_status):
    log_prefix = result['log_prefix']

    if not 'content-length' in headers:
        logger.error('%s url response has no content length' % log_prefix)
        th_status['url_no_length'] = th_status.get('url_no_length', 0) + 1
        return

    content_length = int(headers['content-length'])

    if not 'content-type' in headers:
        logger.error('%s url response has no content type' % log_prefix)
        th_status['url_no_type'] = th_status.get('url_no_type', 0) + 1
        return

    url_file_info = {
        'size': content_length,
        'content_type': headers['content-type'],
    }

    return url_file_info


def download_from_url(url):
    parse_result = urlparse(url)
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


def pipe_file(result, http_url, th_status):
    log_prefix = result['log_prefix']
    key = result['s3_key']
    url = result['url']

    url_file_info = result['url_file_info']
    file_size = url_file_info['size']

    def callback(progress):
        piped_size = progress['piped_size']
        th_status['progress'] = (
            piped_size, file_size, (piped_size + 1.0) / (file_size + 1))

        curr_second = int(time.time())
        index = curr_second % len(REAL_SPEED)
        next_index = (curr_second + 1) % len(REAL_SPEED)

        REAL_SPEED[next_index] = 0
        REAL_SPEED[index] += progress['tokens']

    try:
        uploader.s3_upload(http_read=http_url, s3_key=key,
                           file_size=file_size,
                           content_type=url_file_info['content_type'],
                           token_bucket=tb,
                           s3_signer=s3_signer,
                           bucket_name=cnf['BUCKET_NAME'],
                           endpoint=cnf['ENDPOINT'],
                           multipart_threshold=cnf['MULTIPART_THRESHOLD'],
                           callback=callback)
    except Exception as e:
        logger.exception('%s failed to pipe %s: %s' %
                         (log_prefix, url, repr(e)))
        result['pipe_failed'] = True
        return False

    return True


def _sync_one_file(result, th_status):
    log_prefix = result['log_prefix']
    url = result['url']
    key = result['s3_key']

    status, headers, http_url = download_from_url(url)

    if status != 200:
        body = http_url.read_body(1024)
        message = (('%s got invalid response when doload file: %s ' +
                    'from url, %s, %s, %s') %
                   (log_prefix, key, repr(status), repr(headers), body))
        th_status['url_get_error'] = th_status.get('url_get_error', 0) + 1
        raise DownloadFromUrlError(message)

    url_file_info = extract_url_file_info(headers, result, th_status)
    if url_file_info == None:
        result['pipe_failed'] = True
        raise ExtractFileInfoError('no content length or no content type')

    result['url_file_info'] = url_file_info

    need = base.check_if_need_sync(
        conf=cnf, file_info=url_file_info, bucket_name=cnf['BUCKET_NAME'],
        s3_key=key, s3_client=s3_client, log_prefix=log_prefix,
        status=th_status)

    if not need:
        result['not_need'] = True
        logger.info('%s do not need to sync file: %s' %
                    (log_prefix, key))
        return result

    succeed = pipe_file(result, http_url, th_status)
    if not succeed:
        result['pipe_failed'] = True
        return result

    succeed = base.compare_file(
        bucket_name=cnf['BUCKET_NAME'], s3_key=key, file_info=url_file_info,
        s3_client=s3_client, log_prefix=log_prefix, status=th_status)

    if not succeed:
        result['compare_failed'] = True
        return result

    logger.info('%s finished to sync file: %s' %
                (log_prefix, key))

    return result


def sync_one_file(url_key):
    url = url_key[0]
    key = url_key[1]

    result = {
        'url': url,
        's3_key': key,
    }
    thread_name = threading.current_thread().getName()
    THREAD_STATUS[thread_name] = THREAD_STATUS.get(thread_name, {})
    th_status = THREAD_STATUS[thread_name]

    try:
        result['log_prefix'] = util.get_log_prefix(url)

        logger.info('%s about to sync url: %s' %
                    (result['log_prefix'], url))

        th_status['sync_n'] = th_status.get('sync_n', 0) + 1

        _sync_one_file(result, th_status)

        return result

    except Exception as e:
        logger.exception('got exception when sync one url %s: %s' %
                         (url, repr(e)))
        result['exception'] = True
        th_status['exception'] = th_status.get('exception', 0) + 1

        return result


def update_sync_stat(result):
    SYNC_STATUS['total_n'] += 1

    if 'exception' in result:
        SYNC_STATUS['exception_n'] += 1

    if 'not_need' in result:
        SYNC_STATUS['not_need_n'] += 1

    if 'pipe_failed' in result:
        SYNC_STATUS['pipe_failed_n'] += 1

    if 'compare_failed' in result:
        SYNC_STATUS['compare_failed_n'] += 1


def report_state():
    os.system('clear')
    print '----------------report-----------------'
    print ('speed configuration:(MB)')
    print ', '.join(['%d: %.1f' % (i, speeds[i]) for i in range(24)])

    print ('iter status: total: %d, marker: %s' %
           (ITER_STATUS['iter_n'], ITER_STATUS['marker']))

    curr_index = int(time.time()) % len(REAL_SPEED)
    print ('real speeds: %s' %
           ', '.join(['%.3f' %
                      (REAL_SPEED[(i + curr_index + 2) %
                                  len(REAL_SPEED)] / 1.0 / MB)
                      for i in range(len(REAL_SPEED))]))

    print ('                total: %d' % SYNC_STATUS['total_n'])

    print ('            exception: %d' % SYNC_STATUS['exception_n'])

    print ('             not need: %d' % SYNC_STATUS['not_need_n'])

    print ('          pipe failed: %d' % SYNC_STATUS['pipe_failed_n'])

    print ('       compare failed: %d' % SYNC_STATUS['compare_failed_n'])

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

        jobq.run(iter_url(), [(sync_one_file, cnf['THREADS_NUM']),
                              (update_sync_stat, 1),
                              ])

        sess['stop'] = True
        report_th.join()

        report_state()

    except KeyboardInterrupt:
        report_state()
        sys.exit(0)


def load_cli_args():
    parser = argparse.ArgumentParser(description='sync files from url list')
    parser.add_argument('--conf_path', type=str,
                        help='set the path of the conf file')

    parser.add_argument('--url_list_file', type=str,
                        help='set the path of the url list file')
    args = parser.parse_args()
    return args


def load_conf_from_file(path):
    with open(path) as f:
        conf = yaml.safe_load(f.read())

    return conf


def load_conf(args):
    conf_path = args.conf_path or '../conf/url_sync.yaml'
    conf = load_conf_from_file(conf_path)

    if args.url_list_file is not None:
        conf['URL_LIST_FILE'] = args.url_list_file

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

    s3_signer = awssign.Signer(cnf['ACCESS_KEY'],
                               cnf['SECRET_KEY'])

    fsutil.makedirs(cnf['LOG_DIR'])

    log_file_name = 'url-list-sync-log-for-%s.log' % cnf['BUCKET_NAME']
    logger = util.add_logger(cnf['LOG_DIR'], log_file_name)

    sync()
