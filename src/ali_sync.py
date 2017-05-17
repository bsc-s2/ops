#!/usr/bin/env python2
# coding:utf-8

import base64
import copy
import errno
import getopt
import json
import logging
import os
import sys
import threading
import time
import traceback

import boto3
import oss2
import yaml
from botocore.client import Config

from pykit import jobq

report_state_lock = threading.RLock()

ali_sync_state = {
    'total_n': 0,
    'total_bytes': 0,
    'no_content_md5': 0,
    'no_content_md5_list': [],

    'exist': 0,
    'check_need_s3_error': 0,
    'check_need_s3_error_list': [],
    'size_override': 0,
    'md5_equal': 0,
    'default_override': 0,
    'default_not_override': 0,

    'piped': 0,
    'piped_bytes': 0,
    'pipe_succeed': 0,
    'pipe_succeed_bytes': 0,
    'pipe_failed': 0,
    'pipe_failed_bytes': 0,
    'pipe_failed_exception_error': 0,
    'pipe_failed_exception_error_list': [],
    'pipe_failed_ali_file_size_error': 0,
    'pipe_failed_ali_file_size_error_list': [],
    'pipe_failed_ali_md5_error': 0,
    'pipe_failed_ali_md5_error_list': [],

    'compared': 0,
    'compare_succeed': 0,
    'compare_failed': 0,
    'compare_failed_not_found_error': 0,
    'compare_failed_not_found_error_list': [],
    'compare_failed_exception_error': 0,
    'compare_failed_exception_error_list': [],
    'compare_failed_size_error': 0,
    'compare_failed_size_error_list': [],
    'compare_failed_content_type_error': 0,
    'compare_failed_content_type_error_list': [],
    'compare_failed_content_md5_error': 0,
    'compare_failed_content_md5_error_list': [],
}

ali_meta_prefix = 'x-oss-meta-'


def add_logger():

    log_file = os.path.join(cnf['LOG_DIR'], 'ali-sync-for-' +
                            cnf['ALI_BUCKET_NAME'] + '.log')

    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    file_handler = logging.FileHandler(log_file)
    formatter = logging.Formatter('[%(asctime)s, %(levelname)s] %(message)s')

    file_handler.setFormatter(formatter)

    logger.addHandler(file_handler)

    return logger


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


def get_conf(conf_path):

    with open(conf_path) as f:
        conf = yaml.safe_load(f.read())

    return conf


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
    if os.path.isfile(cnf['PROGRESS_FILE']):
        with open(cnf['PROGRESS_FILE'], 'r') as progress_file:
            progress = json.loads(progress_file.read())

        return progress

    return {
        'marker': '',
        'total_n': 0,
        'total_size': 0,
    }


def store_progress():
    with open(cnf['PROGRESS_FILE'], 'w') as progress_file:
        progress_file.write(json.dumps(current_progress))


def clear_progress():
    os.remove(cnf['PROGRESS_FILE'])


def iter_files():
    marker = current_progress['marker']

    start_marker = cnf.get('START_MARKER', '')
    if start_marker > marker:
        marker = start_marker

    end_marker = cnf.get('END_MARKER', None)

    for file_object in oss2.ObjectIterator(oss2_bucket, prefix=cnf['PREFIX'],  marker=marker):

        if end_marker and file_object.key > end_marker:
            break

        yield file_object

        current_progress['total_n'] += 1
        current_progress['total_size'] += file_object.size
        current_progress['marker'] = file_object.key

        if current_progress['total_n'] % 10000 == 0:
            store_progress()

    store_progress()


def get_ali_user_meta(headers):
    meta = {}
    for k, v in headers.iteritems():
        if k.lower().startswith(ali_meta_prefix):
            meta_name = k.lower()[len(ali_meta_prefix):]
            meta[meta_name] = v

    return meta


def validate_and_extract_ali_file_info(resp_object, result):
    file_object = result['file_object']

    if resp_object.content_length != file_object.size:
        result['pipe_failed_ali_file_size_error'] = {
            'key': file_object.key,
            'content_length': resp_object.content_length,
            'size': file_object.size,
        }
        logger.warn('ali file size error' +
                    repr(result['ali_file_size_error']))
        return

    ali_file_info = {
        'size': file_object.size,
        'content_type': resp_object.content_type,
    }

    if 'Content-MD5' in resp_object.headers:
        md5 = base64.b64decode(resp_object.headers['Content-MD5'])
        md5 = md5.encode('hex')

        if md5 != file_object.etag.lower():
            result['pipe_failed_ali_md5_error'] = {
                'key': file_object.key,
                'content_md5': md5,
                'etag': file_object.etag.lower(),
            }
            logger.warn('ali md5 error' + repr(result['ali_md5_error']))
            return

        ali_file_info['content_md5'] = md5

    else:
        result['no_content_md5'] = {
            'key': file_object.key,
            'object_type': file_object.type,
        }

    ali_file_info['meta'] = get_ali_user_meta(resp_object.headers)

    return ali_file_info


def get_s3_file_info(s3_key):
    resp = s3_client.head_object(
        Bucket=cnf['BAISHAN_BUCKET_NAME'],
        Key=s3_key,
    )

    s3_file_info = {
        'size': resp['ContentLength'],
        'content_type': resp['ContentType'],
        'meta': resp['Metadata'],
        'content_md5': resp['ETag'].lower().strip('"'),
    }

    return s3_file_info


def compare_file_info(ali_file_info, s3_file_info, result, th_status):
    if ali_file_info['size'] != s3_file_info['size']:
        th_status['compare_failed_size_error_n'] = th_status.get(
            'compare_failed_size_error_n', 0) + 1
        result['compare_failed_size_error'] = {
            'key': result['file_object'].key,
            'ali_file_size': ali_file_info['size'],
            's3_file_size': s3_file_info['size'],
        }
        return False

    if ali_file_info['content_type'] != s3_file_info['content_type']:
        th_status['compare_failed_content_type_error_n'] = th_status.get(
            'compare_failed_content_type_error_n', 0) + 1
        result['compare_failed_content_type_error'] = {
            'key': result['file_object'].key,
            'ali_content_type': ali_file_info['content_type'],
            's3_content_type': s3_file_info['content_type'],
        }
        return False

    for k, v in ali_file_info['meta'].iteritems():
        if k not in s3_file_info['meta'] or v != s3_file_info['meta'][k]:
            th_status['compare_failed_meta_error_n'] = th_status.get(
                'compare_failed_meta_error_n', 0) + 1
            result['compate_failed_meta_error'] = {
                'key': result['file_object'].key,
                'ali_meta': repr(ali_file_info['meta']),
                's3_meta': repr(s3_file_info['meta']),
            }
            return False

    if 'content_md5' in ali_file_info:
        if ali_file_info['content_md5'] != s3_file_info['content_md5']:
            th_status['compare_failed_content_md5_error_n'] = th_status.get(
                'compare_failed_content_md5_error_n', 0) + 1
            result['compare_failed_content_md5_error'] = {
                'key': result['file_object'].key,
                'ali_content_md5': ali_file_info['content_md5'],
                's3_content_md5': s3_file_info['content_md5'],
            }
            return False

    return True


def compare_file(result, th_status):
    result['compared'] = True
    th_status['compared_n'] = th_status.get('compared_n', 0) + 1

    try:
        s3_file_info = get_s3_file_info(result['s3_key'])

    except Exception as e:
        result['compare_failed'] = True
        th_status['compare_failed_n'] = th_status.get(
            'compare_failed_n', 0) + 1

        if hasattr(e, 'message') and 'Not Found' in e.message:
            result['compare_failed_not_found_error'] = True
            th_status['compare_failed_not_found_n'] = th_status.get(
                'compare_failed_not_found_n', 0) + 1

            result['compare_failed_not_found_err'] = {
                'key': result['file_object'].key,
                'error': repr(e),
            }
            logger.error('file not exist is s3 when compare file %s: %s' %
                         (result['s3_key'], traceback.format_exc()))
        else:
            result['compare_failed_exception_error'] = True
            th_status['compare_failed_exception_n'] = th_status.get(
                'compare_failed_exception_n', 0) + 1

            result['compare_failed_exception_error'] = {
                'key': result['file_object'].key,
                'error': repr(e),
            }
            logger.error('got exception when get s3 file info %s: %s' %
                         (result['s3_key'], traceback.format_exc()))

        return False

    ali_file_info = result['ali_file_info']

    if not compare_file_info(ali_file_info, s3_file_info, result, th_status):
        result['compared_failed'] = True
        th_status['compare_failed_n'] = th_status.get(
            'compare_failed_n', 0) + 1
        return False

    result['compare_succeed'] = True
    th_status['compare_succeed_n'] = th_status.get('compare_succeed_n', 0) + 1

    return True


def pipe_file(result, th_status):
    result['piped'] = True
    th_status['piped_n'] = th_status.get('piped_n', 0) + 1

    def update_pipe_progress(done_bytes, total_bytes):
        th_status['pipe_progress'] = (done_bytes, total_bytes)

    file_object = result['file_object']

    try:
        resp_object = oss2_bucket.get_object(
            file_object.key, progress_callback=update_pipe_progress)

        ali_file_info = validate_and_extract_ali_file_info(resp_object, result)
        if ali_file_info == None:

            result['pipe_failed'] = True
            th_status['pipe_failed_n'] = th_status.get('pipe_failed_n', 0) + 1
            return False

        extra_args = {
            'ACL': cnf['FILE_ACL'],
            'ContentType': ali_file_info['content_type'],
            'Metadata': ali_file_info['meta'],
        }
        s3_client.upload_fileobj(resp_object, cnf['BAISHAN_BUCKET_NAME'],
                                 result['s3_key'], ExtraArgs=extra_args)

        result['pipe_succeed'] = True
        th_status['pipe_succeed_n'] = th_status.get('pipe_succeed_n', 0) + 1

        result['ali_file_info'] = ali_file_info
        return True

    except Exception as e:
        result['pipe_failed'] = True
        th_status['pipe_failed_n'] = th_status.get('pipe_failed_n', 0) + 1

        result['pipe_failed_exception_error'] = {
            'key': file_object.key,
            'error': repr(e),
        }
        logger.error('got exception when pipe file %s: %s' %
                     (file_object.key, traceback.format_exc()))
        return False


def convert_key(key):
    return key


def check_need(result, th_status):
    if not cnf['CHECK_EXIST']:
        return True

    file_object = result['file_object']

    try:
        s3_file_info = get_s3_file_info(result['s3_key'])

    except Exception as e:
        if hasattr(e, 'message') and 'Not Found' in e.message:
            return True
        else:
            th_status['check_need_s3_error_n'] = th_status.get(
                'check_need_s3_error_n', 0) + 1
            result['check_need_s3_error'] = {
                'key': result['s3_key'],
                'error': repr(e),
            }
            logger.error('faied to get s3 file info in check need %s: %s' %
                         (result['s3_key'], traceback.format_exc()))
            return False

    result['exist'] = True
    th_status['exist_n'] = th_status.get('exist_n', 0) + 1

    if s3_file_info['size'] != file_object.size:
        result['size_override'] = True
        th_status['size_override_n'] = th_status.get('size_override_n', 0) + 1
        logger.info(('need to override file: %s, because size not equal, ' +
                     'ali_size: %d, s3_size: %d') %
                    (result['s3_key'], file_object.size, s3_file_info['size']))
        return True

    if s3_file_info['content_md5'].lower() == file_object.etag.lower():
        result['md5_equal'] = True
        th_status['md5_equal_n'] = th_status.get('md5_equal_n', 0) + 1
        return False

    if cnf['OVERRIDE']:
        result['default_override'] = True
        th_status['default_override_n'] = th_status.get(
            'default_override_n', 0) + 1
        return True
    else:
        result['default_not_override'] = True
        th_status['default_not_override_n'] = th_status.get(
            'default_not_override_n', 0) + 1
        return False


def sync_one_file(file_object):
    thread_name = threading.current_thread().getName()
    thread_status[thread_name] = thread_status.get(thread_name, {})
    th_status = thread_status[thread_name]

    th_status['total_n'] = th_status.get('total_n', 0) + 1

    result = {
        'file_object': file_object,
        's3_key': convert_key(file_object.key)
    }

    if not check_need(result, th_status):
        return result

    if not pipe_file(result, th_status):
        return result

    if not compare_file(result, th_status):
        return result

    return result


def update_sync_stat(result):
    file_object = result['file_object']

    ali_sync_state['total_n'] += 1
    ali_sync_state['total_bytes'] += file_object.size

    if 'no_content_md5' in result:
        ali_sync_state['no_content_md5'] += 1
        ali_sync_state['no_content_md5_list'].append(result['no_content_md5'])

    if 'check_need_s3_error' in result:
        ali_sync_state['check_need_s3_error'] += 1
        ali_sync_state['check_need_s3_error_list'].append(
            result['check_need_s3_error'])
        return

    if 'exist' in result:
        ali_sync_state['exist'] += 1

        if 'size_override' in result:
            ali_sync_state['size_override'] += 1

        elif 'md5_equal' in result:
            ali_sync_state['md5_equal'] += 1

        elif 'default_override' in result:
            ali_sync_state['default_override'] += 1

        elif 'default_not_override' in result:
            ali_sync_state['default_not_override'] += 1

    if not 'piped' in result:
        return

    ali_sync_state['piped'] += 1
    ali_sync_state['piped_bytes'] += file_object.size

    if 'pipe_failed' in result:
        ali_sync_state['pipe_failed'] += 1
        ali_sync_state['pipe_failed_bytes'] += file_object.size

        if 'pipe_failed_exception_error' in result:
            ali_sync_state['pipe_failed_exception_error'] += 1
            ali_sync_state['pipe_failed_exception_error_list'].append(
                result['pipe_failed_exception_error'])

        elif 'pipe_failed_ali_file_size_error' in result:
            ali_sync_state['pipe_failed_ali_file_size_error'] += 1
            ali_sync_state['pipe_failed_ali_file_size_error_list'].append(
                result['pipe_failed_ali_file_size_error'])

        elif 'pipe_failed_ali_md5_error' in result:
            ali_sync_state['pipe_failed_ali_md5_error'] += 1
            ali_sync_state['pipe_failed_ali_md5_error_list'].append(
                result['pipe_failed_ali_md5_error'])

        return

    ali_sync_state['pipe_succeed'] += 1
    ali_sync_state['pipe_succeed_bytes'] += file_object.size

    if not 'compared' in result:
        return

    if 'compare_failed' in result:
        ali_sync_state['compare_failed'] += 1

        if 'compare_failed_not_found_error' in result:
            ali_sync_state['compare_failed_not_found_error'] += 1
            ali_sync_state['compare_failed_not_found_error_list'].append(
                result['compare_failed_not_found_error'])

        elif 'compare_failed_exception_error' in result:
            ali_sync_state['compare_failed_exception_error'] += 1
            ali_sync_state['compare_failed_exception_error_list'].append(
                result['compare_failed_exception_error'])

        elif 'compare_failed_size_error' in result:
            ali_sync_state['compare_failed_size_error'] += 1
            ali_sync_state['compare_failed_size_error_list'].append(
                result['compare_failed_size_error'])

        elif 'compare_failed_content_md5_error' in result:
            ali_sync_state['compare_failed_content_md5_error'] += 1
            ali_sync_state['compare_failed_exception_error_list'].append(
                result['compare_failed_content_md5_error'])

        return

    ali_sync_state['compare_succeed'] += 1


def report_thread_status(th_status):
    total_n = th_status.get('total_n', 0)
    s3_error_n = th_status.get('check_need_s3_error_n', 0)
    exist_n = th_status.get('exist_n', 0)
    size_override_n = th_status.get('size_override_n', 0)
    md5_equal_n = th_status.get('md5_equal_n', 0)
    d_override_n = th_status.get('default_override_n', 0)
    d_not_override_n = th_status.get('default_not_override_n', 0)

    piped_n = th_status.get('piped_n', 0)
    pipe_succeed_n = th_status.get('pipe_succeed_n', 0)
    pipe_failed_n = th_status.get('pipe_failed_n', 0)
    pipe_progress = th_status.get('pipe_progress', (0, 0))

    compared_n = th_status.get('compared_n', 0)
    compare_succeed_n = th_status.get('compare_succeed_n', 0)
    compare_failed_n = th_status.get('compare_failed_n', 0)
    not_found_n = th_status.get('compare_failed_not_found_n', 0)
    exception_n = th_status.get('compare_failed_exception_n', 0)
    size_error_n = th_status.get('compare_failed_size_error_n', 0)
    content_type_error_n = th_status.get(
        'compare_failed_content_typ_error_n', 0)
    meta_error_n = th_status.get('compate_failed_meta_error_n', 0)
    content_md5_error_n = th_status.get(
        'compare_failed_content_md5_error_n', 0)

    print (('total: %d, get s3 file info failed: %s, exist: %d, size ' +
            'override: %d, md5_equal: %d, default override: %d, default' +
            'not override: %d ') %
           (total_n, s3_error_n, exist_n, size_override_n, md5_equal_n,
            d_override_n, d_not_override_n))

    print ('piped: %d, pipe succeed: %d, pipe failed: %d, pipe grogress: %s' %
           (piped_n, pipe_succeed_n, pipe_failed_n, repr(pipe_progress)))

    print (('compared: %d, compare succeed: %d, compare failed: %d, not ' +
            'found: %d, exception: %d, size error: %d, type error: %d, ' +
            'meta error: %d, md5 error: %d') %
           (compared_n, compare_succeed_n, compare_failed_n, not_found_n,
            exception_n, size_error_n, content_type_error_n,
            meta_error_n, content_md5_error_n))


def _report_state():
    # os.system('clear')
    print (('ali bucket name: %s, prefix: %s, start marker: %s, ' +
            'end marker: %s,  baishan bucket name: %s') %
           (cnf['ALI_BUCKET_NAME'], cnf['PREFIX'], cnf['START_MARKER'],
            cnf['END_MARKER'], cnf['BAISHAN_BUCKET_NAME']))

    print ''
    print (('previous iter progress: total number: %d, ' +
            'total size: %d, marker: %s') %
           (previous_progress['total_n'],
            previous_progress['total_size'],
            previous_progress['marker']))

    print (('current iter progress: total number: %d, ' +
            'total size: %d, marker: %s') %
           (current_progress['total_n'],
            current_progress['total_size'],
            current_progress['marker']))

    print ''
    print ('total number: %d,  total bytes: %d, no content md5: %d' %
           (ali_sync_state['total_n'], ali_sync_state['total_bytes'],
            ali_sync_state['no_content_md5']))
    print ''

    print 'check exist: %s' % cnf['CHECK_EXIST']
    print 'get s3 file info failed: %d' % ali_sync_state['check_need_s3_error']
    print (('exist: %d, size_override: %d, md5_equal: %d, ' +
            'default_override: %d, default_not_override: %d') %
           (ali_sync_state['exist'],
            ali_sync_state['size_override'],
            ali_sync_state['md5_equal'],
            ali_sync_state['default_override'],
            ali_sync_state['default_not_override']))

    print ''
    print 'piped: %d, piped_bytes: %d' % (ali_sync_state['piped'],
                                          ali_sync_state['piped_bytes'])

    print ('pipe succeed: %d, pipe succeed bytes: %d' %
           (ali_sync_state['pipe_succeed'],
            ali_sync_state['pipe_succeed_bytes']))
    print ('pipe failed: %d, pipe failed bytes: %d' %
           (ali_sync_state['pipe_failed'],
            ali_sync_state['pipe_failed_bytes']))

    print (('pipe failed reason: exception: %d, ali file size error: %d, ' +
            'ali md5 error: %d') %
           (ali_sync_state['pipe_failed_exception_error'],
            ali_sync_state['pipe_failed_ali_file_size_error'],
            ali_sync_state['pipe_failed_ali_md5_error']))

    print ''
    print ('compared: %d, compare_succeed: %d, compare_failed: %d' %
           (ali_sync_state['compared'],
            ali_sync_state['compare_succeed'],
            ali_sync_state['compare_failed']))

    print (('compare failed reason: not found: %d, exception: %d, ' +
            'size error: %d, content type error: %d, content md5 error: %d') %
           (ali_sync_state['compare_failed_not_found_error'],
            ali_sync_state['compare_failed_exception_error'],
            ali_sync_state['compare_failed_size_error'],
            ali_sync_state['compare_failed_content_type_error'],
            ali_sync_state['compare_failed_content_md5_error']))

    print ''
    print 'threads status:'
    for th_name, th_status in thread_status.iteritems():
        print th_name
        report_thread_status(th_status)
        print ''


def report_state():
    with report_state_lock:
        _report_state()


def report(sess):
    while not sess['stop']:
        report_state()
        time.sleep(cnf['REPORT_INTERVAL'])


def dump_state():
    with open(cnf['STATE_FILE'], 'w') as stat_file:
        stat_file.write(json.dumps(ali_sync_state))


def sync():
    try:
        report_sess = {'stop': False}
        report_th = _thread(report, (report_sess,))
        jobq.run(iter_files(), [(sync_one_file, 3),
                                (update_sync_stat, 1),
                                ])

        report_sess['stop'] = True
        report_th.join()

        report_state()
        dump_state()

    except KeyboardInterrupt:
        report_state()
        dump_state()
        sys.exit(0)


if __name__ == "__main__":

    opts, args = getopt.getopt(sys.argv[1:], '', ['conf=', ])
    opts = dict(opts)

    if opts.get('--conf') is None:
        conf_path = '../conf/ali_sync.yaml'
    else:
        conf_path = opts['--conf']

    cnf = get_conf(conf_path)

    oss2_auth = oss2.Auth(cnf['ALI_ACCESS_KEY'], cnf['ALI_SECRET_KEY'])
    oss2_bucket = oss2.Bucket(
        oss2_auth, cnf['ALI_ENDPOINT'], cnf['ALI_BUCKET_NAME'])

    s3_client = get_boto_client(cnf['BAISHAN_ENDPOINT'])

    _mkdir(cnf['LOG_DIR'])

    logger = add_logger()

    thread_status = {}

    cmd = args[0]

    if cmd == 'sync':
        current_progress = load_progress()
        previous_progress = copy.deepcopy(current_progress)
        sync()
    elif cmd == 'clear_progress':
        clear_progress()
