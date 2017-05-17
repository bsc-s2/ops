#!/usr/bin/env python2
# coding:utf-8

import errno
import getopt
import json
import logging
import os
import sys
import traceback

import boto3
import qiniu
import yaml
from botocore.client import Config

from pykit import jobq

add_task_stat = {
    'total': 0,
    'failed': 0,
    'exist': 0,
    'added': 0,
    'override': 0,
    'force_override': 0,
    'fsize_override': 0,
    'content_type_override': 0,
    'case_diff': 0,
    'failed_item_list': [],
}

compare_stat = {
    'total': 0,
    'failed': 0,
    'content_type_error': 0,
    'case_diff': 0,
    'file_size_error': 0,
    'compare_failed_item_list': [],
}


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


def get_boto_client(endpoint):
    session = boto3.session.Session()

    client = session.client(
        's3',
        use_ssl=False,
        aws_access_key_id=cnf['BAISHAN_ACCESS_KEY'],
        aws_secret_access_key=cnf['BAISHAN_SECRET_KEY'],
        config=Config(signature_version='s3v4'),
        region_name='us-east-1',
        endpoint_url=endpoint,
    )

    return client


def list_all_files():
    eof = False

    file_bundle = []
    bundle_index = 0

    marker = cnf['MARKER']
    try:
        while eof is False:
            result, eof, info = qiniu_bucket_manager.list(cnf['QINIU_BUCKET_NAME'],
                                                          prefix=cnf['PREFIX'],
                                                          marker=marker,
                                                          limit=1000)
            for item in result['items']:
                file_bundle.append(item)

                if len(file_bundle) == cnf['FILE_BUNDLE_SIZE']:
                    file_path = os.path.join(
                        cnf['FILE_BUNDLE_DIR'], 'file_bundle_%d.json' % bundle_index)
                    with open(file_path, 'w') as f:
                        f.write(json.dumps(file_bundle))
                    print 'created file bundle: %s, size is: %d' % (file_path, len(file_bundle))

                    bundle_index += 1
                    file_bundle = []

            marker = result.get('marker')

        if len(file_bundle) > 0:
            file_path = os.path.join(
                cnf['FILE_BUNDLE_DIR'], 'file_bundle_%d.json' % bundle_index)
            with open(file_path, 'w') as f:
                f.write(json.dumps(file_bundle))
            print 'created file bundle: %s, size is: %d' % (file_path, len(file_bundle))

        print 'files total number: ' + str(cnf['FILE_BUNDLE_SIZE'] * bundle_index + len(file_bundle))

    except Exception as e:
        logger.error('failed to list file: ' + traceback.format_exc())
        print 'failed to list file: ' + repr(e)


def get_exist_file(key):
    try:
        exist_file = {}

        resp = s3_client.head_object(
            Bucket=cnf['BAISHAN_BUCKET_NAME'],
            Key=key,
        )
        exist_file['fsize'] = int(resp['Metadata']['s2-size'])
        exist_file['mimeType'] = resp['ContentType']

        return exist_file

    except Exception as e:
        logger.info('failed to head object: ' + repr(e))
        return None


def check_if_override(exist_file, item, result):
    if cnf['FORCE_OVERRIDE']:
        result['force_override'] = True
        return True

    if exist_file['fsize'] != item['fsize']:
        result['fsize_override'] = True
        return True

    origin_type = item['mimeType']
    now_type = exist_file['mimeType']

    if origin_type.lower() == now_type.lower() and origin_type != now_type:
        result['case_diff'] = True

    if not cnf['CONTENT_TYPE_CASE_SENSITIVE']:
        origin_type = origin_type.lower()
        now_type = now_type.lower()

    if now_type != origin_type:
        result['content_type_override'] = True
        return True

    return False


def add_one_offline_task(item):
    result = {
        'item': item
    }
    try:
        key = item['key']
        if isinstance(key, unicode):
            key = key.encode('utf-8')

        if cnf['CHECK_EXIST']:
            exist_file = get_exist_file(key)

            if exist_file != None:
                result['exist'] = True

                override = check_if_override(exist_file, item, result)
                if override:
                    result['override'] = True
                else:
                    return result

        result['added'] = True

        url = 'http://%s/%s' % (cnf['QINIU_BUCKET_DOMAIN'], key)
        url = qiniu_auth.private_download_url(url, expires=3600 * 24 * 3)

        body = {
            'Key': key,
            'Url': url,
            'FailureCallbackUrl': 'nocallback',
            'SuccessCallbackUrl': 'nocallback',
            'ACLs': {'acl': cnf['FILE_ACL']},
            'OnKeyExist': 'override',
        }

        body = json.dumps(body)

        logger.info('about to add offline task for: %s, the body is: %s' %
                    (repr(item), repr(body)))

        resp = offline_client.put_object(
            Bucket=cnf['BAISHAN_BUCKET_NAME'],
            Key='nokey',
            Body=body,
        )

        result['task_id'] = resp['ResponseMetadata'][
            'HTTPHeaders']['x-amz-s2-offline-task-id']
        return result

    except Exception as e:
        logger.error('failed to add offline task for: %s, %s' %
                     (repr(item), traceback.format_exc()))
        print 'failed to add offline task for: %s, %s' % (repr(item), repr(e))

        result['error'] = True
        return result


def iter_item_from_bundles(bundle_index_start, bundle_index_end):
    try:
        for index in range(bundle_index_start, bundle_index_end):
            file_path = os.path.join(
                cnf['FILE_BUNDLE_DIR'], 'file_bundle_%d.json' % index)
            print 'start to iter item from: %s' % file_path

            with open(file_path, 'r') as f:
                items = json.loads(f.read())

            for item in items:
                yield item

            print 'finished to iter items from: %s' % file_path

    except Exception as e:
        logger.error('failed to iter item from bundles: ' +
                     traceback.format_exc())
        print 'failed to iter item from bundles: ' + repr(e)


def update_add_task_stat(result):
    add_task_stat['total'] += 1
    if 'error' in result:
        add_task_stat['failed'] += 1
        add_task_stat['failed_item_list'].append(result['item'])
        return

    if 'exist' in result:
        add_task_stat['exist'] += 1

    if 'override' in result:
        add_task_stat['override'] += 1

    if 'force_override' in result:
        add_task_stat['force_override'] += 1

    if 'fsize_override' in result:
        add_task_stat['fsize_override'] += 1

    if 'content_type_override' in result:
        add_task_stat['content_type_override'] += 1

    if 'case_diff' in result:
        add_task_stat['case_diff'] += 1

    if 'added' in result:
        add_task_stat['added'] += 1


def add_offline_tasks(bundle_index_start, bundle_index_end):

    add_task_stat['total'] = 0
    add_task_stat['failed'] = 0
    add_task_stat['failed_item_list'] = []

    jobq.run(iter_item_from_bundles(bundle_index_start, bundle_index_end),
             [(add_one_offline_task, cnf['THREADS_NUM_FOR_ADD_OFFLINE_TASK']),
              (update_add_task_stat, 1),
              ])

    print 'total: %d,  failed: %d, added: %d' % (add_task_stat['total'], add_task_stat['failed'], add_task_stat['added'])
    if cnf['CHECK_EXIST']:
        print ('exist: %d, override: %d, force_override: %d, fsize_override: %d, content_type_override: %d, case_diff: %d' %
               (add_task_stat['exist'], add_task_stat['override'], add_task_stat['force_override'],
                add_task_stat['fsize_override'], add_task_stat['content_type_override'], add_task_stat['case_diff']))

    if add_task_stat['failed'] > 0:
        file_name = 'failed_item_list_for_bundle_%d_to_%d.json' % (
            bundle_index_start, bundle_index_end)

        with open(file_name, 'w') as f:
            f.write(json.dumps(add_task_stat['failed_item_list']))

        print 'writed failed item list to file: ' + file_name


def compare_one_item(item):
    result = {
        'item': item,
    }

    try:

        key = item['key']
        if isinstance(key, unicode):
            key = key.encode('utf-8')

        origin_content_type = item['mimeType']
        if isinstance(origin_content_type, unicode):
            origin_content_type = origin_content_type.encode('utf-8')

        origin_size = item['fsize']

        resp = s3_client.head_object(
            Bucket=cnf['BAISHAN_BUCKET_NAME'],
            Key=key,
        )

        now_content_type = resp['ContentType']
        if origin_content_type.lower() == now_content_type.lower() and origin_content_type != now_content_type:
            result['case_diff'] = True

        if not cnf['CONTENT_TYPE_CASE_SENSITIVE']:
            origin_content_type = origin_content_type.lower()
            now_content_type = now_content_type.lower()

        if origin_content_type != now_content_type:
            log_str = 'the content type not equal for item: %s, origin: %s, now: %s' % (
                repr(item), origin_content_type, now_content_type)
            logger.error(log_str)
            print log_str
            result['content_type_error'] = True
            return result

        if origin_size != int(resp['Metadata']['s2-size']):
            log_str = 'the file size not equal for item: %s, origin: %s, now: %s' % (
                repr(item), origin_size, resp['Metadata']['s2-size'])
            logger.error(log_str)
            print log_str
            result['file_size_error'] = True
            return result

        return result

    except Exception as e:
        logger.error('failed to compare item: %s, %s' %
                     (repr(item), traceback.format_exc()))
        print 'failed to compare item: %s, %s' % (repr(item), repr(e))

        result['error'] = True
        return result


def update_compare_stat(result):
    compare_stat['total'] += 1

    if 'error' in result:
        compare_stat['failed'] += 1
        compare_stat['compare_failed_item_list'].append(result['item'])
        return

    if 'case_diff' in result:
        compare_stat['case_diff'] += 1

    if 'content_type_error' in result:
        compare_stat['content_type_error'] += 1
        compare_stat['compare_failed_item_list'].append(result['item'])
        return

    if 'file_size_error' in result:
        compare_stat['file_size_error'] += 1
        compare_stat['compare_failed_item_list'].append(result['item'])
        return


def compare(bundle_index_start, bundle_index_end):
    compare_stat['total'] = 0
    compare_stat['failed'] = 0
    compare_stat['content_type_error'] = 0
    compare_stat['file_size_error'] = 0

    jobq.run(iter_item_from_bundles(bundle_index_start, bundle_index_end),
             [(compare_one_item, cnf['THREADS_NUM_FOR_COMPARE_FILE']),
              (update_compare_stat, 1),
              ])

    print ('total: %d,  failed: %d, content_type_error: %d, file_size_error: %d, case_diff: %d' %
           (compare_stat['total'], compare_stat['failed'],
            compare_stat['content_type_error'], compare_stat['file_size_error'], compare_stat['case_diff']))

    if len(compare_stat['compare_failed_item_list']) > 0:
        file_name = 'compare_failed_item_list_for_bundle_%d_to_%d.json' % (
            bundle_index_start, bundle_index_end)

        with open(file_name, 'w') as f:
            f.write(json.dumps(compare_stat['compare_failed_item_list']))

        print 'writed compare failed item list to file: ' + file_name


def add_logger():

    log_file = os.path.join(cnf['LOG_DIR'], 'qiniu-sync-offline-for-' +
                            cnf['QINIU_BUCKET_NAME'] + '.log')

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
        conf_path = '../conf/qiniu_sync_offline.yaml'
    else:
        conf_path = opts['--conf']

    cnf = get_conf(conf_path)

    qiniu_auth = qiniu.Auth(cnf['QINIU_ACCESS_KEY'], cnf['QINIU_SECRET_KEY'])
    qiniu_bucket_manager = qiniu.BucketManager(qiniu_auth)

    s3_client = get_boto_client(cnf['BAISHAN_ENDPOINT'])
    offline_client = get_boto_client(cnf['BAISHAN_OFFLINE_ENDPOINT'])

    _mkdir(cnf['LOG_DIR'])
    _mkdir(cnf['FILE_BUNDLE_DIR'])

    logger = add_logger()

    cmd = args[0]

    if cmd == 'list':
        list_all_files()
    elif cmd == 'add_offline_task':
        add_offline_tasks(int(args[1]), int(args[2]))
    elif cmd == 'compare':
        compare(int(args[1]), int(args[2]))
