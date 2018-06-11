#!/usr/bin/env python2
# coding:utf-8

import copy
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

from pykit import fsutil
from pykit import jobq
from pykit import logutil
from pykit import threadutil
from pykit import utfjson
from pykit import utfyaml

report_state_lock = threading.RLock()

logger = logging.getLogger(__name__)

sync_state = {
    'total_n': 0,
    'total_bytes': 0,

    'exist': 0,
    'check_dest_file_error': 0,
    'check_dest_file_error_list': [],
    'config_override': 0,
    'force_override': 0,

    'piped': 0,
    'piped_bytes': 0,
    'pipe_succeed': 0,
    'pipe_succeed_bytes': 0,
    'pipe_failed': 0,
    'pipe_failed_bytes': 0,
    'pipe_failed_exception_error': 0,
    'pipe_failed_exception_error_list': [],

}


def _thread(func, args):
    th = threading.Thread(target=func, args=args)
    th.daemon = True
    th.start()

    return th


def get_conf(conf_path):

    with open(conf_path) as f:
        conf = utfyaml.load(f.read())

    return conf


def get_boto_client(endpoint, access_key, secret_key):

    client = boto3.client(
        's3',
        use_ssl=False,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        config=Config(signature_version='s3v4'),
        region_name='us-east-1',
        endpoint_url=endpoint,
    )

    return client


def load_progress():

    progress_file = cnf['PROGRESS_FILE']

    if os.path.isfile(progress_file):
        progress = utfjson.load(fsutil.read_file(progress_file))
    else:
        progress = {
            'marker': '',
            'total_n': 0,
            'total_size': 0.
        }

    return progress


def store_progress():
    fsutil.write_file(cnf['PROGRESS_FILE'], utfjson.dump(current_progress))


def clear_progress():
    fsutil.remove(cnf['PROGRESS_FILE'])


def get_file_info(client, bucket, key):

    resp = client.head_object(
        Bucket=bucket,
        Key=key,
    )

    return resp


def iter_files(client, bucket):
    marker = current_progress['marker']

    start_marker = cnf.get('START_MARKER', '')
    if start_marker > marker:
        marker = start_marker

    end_marker = cnf.get('END_MARKER', None)

    while True:
        resp = client.list_objects(
            Bucket=bucket,
            Marker=marker,
        )

        if 'Contents' not in resp:
            print 'list file end'
            break
        for content in resp['Contents']:

            if end_marker is not None and content['Key'] >= end_marker:
                print 'list file end marker reacched'
                return

            marker = content['Key']

            yield content

            current_progress['total_n'] += 1
            current_progress['total_size'] += content['Size']
            current_progress['marker'] = content['Key']

            if current_progress['total_n'] % 10000 == 0:
                store_progress()

    store_progress()


def pipe_file(result):

    result['piped'] = True

    file_object = result['file_object']

    try:
        src_resp = src_client.get_object(
            Bucket=cnf['SRC_BUCKET'],
            Key=file_object['Key'],
        )

        dest_body = src_resp['Body'].read()

        dest_object = dest_client.put_object(
            Bucket=cnf['DEST_BUCKET'],
            Key=result['dest_key'],
            ContentLength=src_resp['ContentLength'],
            ContentType=src_resp['ContentType'],
            Metadata=src_resp['Metadata'],
            Body=dest_body,
        )

        if src_resp['ETag'] == dest_object['ETag']:

            result['pipe_succeed'] = True

            result['src_file_info'] = file_object

            return True
        else:
            raise RuntimeError('ETagNotEqual')

    except Exception as e:
        result['pipe_failed'] = True

        result['pipe_failed_exception_error'] = {
            'key': file_object['Key'],
            'error': repr(e),
        }
        logger.error('got exception when pipe file %s: %s' %
                     (file_object['Key'], traceback.format_exc()))
        return False


def convert_key(key):
    return key


def check_dest_file(result):

    try:
        src_file_info = get_file_info(
            src_client, cnf['SRC_BUCKET'], result['dest_key'])

        dest_file_info = get_file_info(
            dest_client, cnf['DEST_BUCKET'], result['dest_key'])

    except Exception as e:
        if hasattr(e, 'message') and 'Not Found' in e.message:

            return True

        else:
            result['check_dest_file_error'] = {
                'key': result['dest_key'],
                'error': repr(e),
            }

            logger.error('faied to get dest file info in {k}: {t}'.format(
                k=repr(result['dest_key']), t=repr(traceback.format_exc())))

            return False

    result['exist'] = True

    if cnf['FORCE_OVERRIDE']:

        result['force_override'] = True

        logger.info('need to override file:{k} because FORCE_OVERRIDE is True'.format(
            k=repr(result['dest_key'])))

        return True

    else:

        for metric in cnf['CONFIG_OVERRIDE']:

            if dest_file_info[metric] != src_file_info[metric]:
                result['config_override'] = True

                logger.info('need to overrid file:{k},because CONFIG_OVERRIDE:{m} is configured'.format(
                    k=repr(result['dest_key']), m=metric))

                return True

        return False


def sync_one_file(file_object):

    result = {
        'file_object': file_object,
        'dest_key': convert_key(file_object['Key'])
    }

    check_dest_file(result)

    if not check_dest_file(result):
        return result

    if not pipe_file(result):
        return result

    return result


def update_sync_stat(result):

    file_object = result['file_object']

    sync_state['total_n'] += 1
    sync_state['total_bytes'] += file_object['Size']

    if 'check_dest_file_error' in result:
        sync_state['check_dest_file_error'] += 1
        sync_state['check_dest_file_error_list'].append(
            result['check_dest_file_error'])
        return

    if 'exist' in result:

        sync_state['exist'] += 1

        if 'config_override' in result:
            sync_state['config_override'] += 1

        elif 'force_override' in result:
            sync_state['force_override'] += 1

    if not 'piped' in result:
        return

    sync_state['piped'] += 1
    sync_state['piped_bytes'] += file_object['Size']

    if 'pipe_failed' in result:
        sync_state['pipe_failed'] += 1
        sync_state['pipe_failed_bytes'] += file_object['Size']

        if 'pipe_failed_exception_error' in result:
            sync_state['pipe_failed_exception_error'] += 1
            sync_state['pipe_failed_exception_error_list'].append(
                result['pipe_failed_exception_error'])

        return

    sync_state['pipe_succeed'] += 1
    sync_state['pipe_succeed_bytes'] += file_object['Size']


def _report_state():
    os.system('clear')
    print (('src bucket name: %s, prefix: %s, start marker: %s, ' +
            'end marker: %s,  dest bucket name: %s') %
           (cnf['SRC_BUCKET'], cnf['PREFIX'], cnf['START_MARKER'],
            cnf['END_MARKER'], cnf['DEST_BUCKET']))

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
    print ('total number: %d,  total bytes: %d' %
           (sync_state['total_n'], sync_state['total_bytes']))
    print ''

    print 'get dest file info failed: %d' % sync_state['check_dest_file_error']
    print (('exist: %d, config_override: %d, force_override: %d, ') %
           (sync_state['exist'],
            sync_state['config_override'],
            sync_state['force_override'],
            ))

    print ''
    print 'piped: %d, piped_bytes: %d' % (sync_state['piped'],
                                          sync_state['piped_bytes'])

    print ('pipe succeed: %d, pipe succeed bytes: %d' %
           (sync_state['pipe_succeed'],
            sync_state['pipe_succeed_bytes']))
    print ('pipe failed: %d, pipe failed bytes: %d' %
           (sync_state['pipe_failed'],
            sync_state['pipe_failed_bytes']))


def report_state():
    with report_state_lock:
        _report_state()


def report(sess):
    while not sess['stop']:
        report_state()
        time.sleep(cnf['REPORT_INTERVAL'])


def dump_state():
    fsutil.write_file(cnf['STATE_FILE'], utfjson.dump(sync_state))


def sync():

    try:
        report_sess = {'stop': False}
        report_th = threadutil.start_thread(
            report, args=(report_sess,), daemon=True)

        jobq.run(iter_files(src_client, cnf['SRC_BUCKET']), [(sync_one_file, 3),
                                                             (update_sync_stat, 1),
                                                             ])

        report_sess['stop'] = True
        report_th.join()

    except KeyboardInterrupt:
        logger.exception('get KeyboardInterrupt')
        sys.exit(0)

    finally:
        report_state()
        dump_state()


if __name__ == "__main__":

    logutil.make_logger(base_dir='/var/log/opstool', level='INFO')

    opts, args = getopt.getopt(sys.argv[1:], '', ['conf=', ])
    opts = dict(opts)

    if opts.get('--conf') is None:
        conf_path = '../conf/sync_cross_cluster.yaml'
    else:
        conf_path = opts['--conf']

    cnf = get_conf(conf_path)

    src_client = get_boto_client(
        cnf['SRC_ENDPOINT'],
        cnf['SRC_ACCESS_KEY'],
        cnf['SRC_SECRET_KEY'])

    dest_client = get_boto_client(
        cnf['DEST_ENDPOINT'],
        cnf['DEST_ACCESS_KEY'],
        cnf['DEST_SECRET_KEY'])

    thread_status = {}

    cmd = args[0]

    if cmd == 'sync':
        current_progress = load_progress()
        previous_progress = copy.deepcopy(current_progress)
        sync()
    elif cmd == 'clear_progress':
        clear_progress()
