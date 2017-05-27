#!/usr/bin/env python2
# coding: utf-8

import argparse
import errno
import logging
import os
import threading
import time

import boto3
import yaml
from botocore.client import Config

from pykit import jobq

ITER_STATUS = {
    'total': 0,
    'total_size': 0,
    'marker': '',
}


MOVE_STATUS = {
    'total': 0,
    'total_size': 0,
}

PERM_TO_ARG = {
    'READ': 'GrantRead',
    'READ_ACP': 'GrantReadACP',
    'WRITE': 'GrantWrite',
    'WRITE_ACP': 'GrantWriteACP',
    'FULL_CONTROL': 'GrantFullControl',
}


class MoveFileError(Exception):
    pass


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


def load_conf_from_file(path):
    with open(path) as f:
        conf = yaml.safe_load(f.read())

    return conf


def boto_client():
    session = boto3.session.Session()

    client = session.client(
        's3',
        use_ssl=False,
        aws_access_key_id=cnf['ACCESS_KEY'],
        aws_secret_access_key=cnf['SECRET_KEY'],
        config=Config(signature_version='s3v4'),
        region_name='us-east-1',
        endpoint_url=cnf['ENDPOINT'],
    )

    return client


def add_logger():

    log_file = os.path.join(cnf['LOG_DIR'], 'move-file-log-for-' +
                            cnf['SRC_BUCKET'] + '.log')

    log = logging.getLogger()
    log.setLevel(logging.INFO)

    file_handler = logging.FileHandler(log_file)
    formatter = logging.Formatter('[%(asctime)s, %(levelname)s] %(message)s')

    file_handler.setFormatter(formatter)

    log.addHandler(file_handler)

    return log


def iter_file():
    num_limit = cnf['NUM_LIMIT']

    start_marker = cnf['START_MARKER']
    end_marker = cnf['END_MARKER']
    prefix = cnf['OLD_PREFIX']

    marker = start_marker

    n = 0
    try:
        while True:
            resp = s3_client.list_objects(
                Bucket=cnf['SRC_BUCKET'],
                Marker=marker,
                Prefix=prefix,
            )

            if 'Contents' not in resp:
                print 'list file end'
                break

            for content in resp['Contents']:
                if num_limit is not None and n >= num_limit:
                    print 'list file limit reached'
                    return

                if end_marker is not None and content['Key'] >= end_marker:
                    print 'list file end marker reached'
                    return

                marker = content['Key']

                ITER_STATUS['total'] += 1
                ITER_STATUS['total_size'] += content['Size']
                ITER_STATUS['marker'] = marker

                yield {
                    'key_name': content['Key'],
                    'size': content['Size'],
                }

                n += 1

    except Exception as e:
        logger.exception('failed to iter file: ' + repr(e))
        print 'iter file exception: ' + repr(e)


def change_key_name(old_key_name):
    old_prefix_len = len(cnf['OLD_PREFIX'])

    new_key_name = cnf['NEW_PREFIX'] + old_key_name[old_prefix_len:]

    return new_key_name


def build_grants_args(acl_resp):
    grants = {
        'READ': {
            'CanonicalUser': [],
            'Group': [],
        },
        'READ_ACP': {
            'CanonicalUser': [],
            'Group': [],
        },
        'WRITE': {
            'CanonicalUser': [],
            'Group': [],
        },
        'WRITE_ACP': {
            'CanonicalUser': [],
            'Group': [],
        },
        'FULL_CONTROL': {
            'CanonicalUser': [],
            'Group': [],
        },
    }

    for grant in acl_resp['Grants']:
        permission = grant['Permission']
        grantee = grant['Grantee']

        if grantee['Type'] == 'Group':
            grants[permission]['Group'].append(grantee['URI'])

        elif grantee['Type'] == 'CanonicalUser':
            grants[permission]['CanonicalUser'].append(grantee['ID'])

    grant_args = {}
    for permission, grantees in grants.iteritems():
        arg_value = ','.join(
            ['uri="%s"' % group for group in grantees['Group']] +
            ['id="%s"' % user for user in grantees['CanonicalUser']])

        if arg_value != '':
            grant_args[PERM_TO_ARG[permission]] = arg_value

    return grant_args


def get_old_file_acl_grants_args(old_key_name):
    acl_resp = s3_client.get_object_acl(
        Bucket=cnf['SRC_BUCKET'],
        Key=old_key_name,
    )

    grant_args = build_grants_args(acl_resp)
    return grant_args


def copy_file(result):
    old_key_name = result['file_info']['key_name']

    new_key_name = change_key_name(old_key_name)

    try:
        if cnf['COPY_ACL']:
            grant_args = get_old_file_acl_grants_args(old_key_name)
        else:
            grant_args = {}

    except Exception as e:
        logger.exception('failed to get acl of file: ' + old_key_name)
        result['state'] = 'get_acl_error'

        raise MoveFileError(repr(e))

    if 'GrantWrite' in grant_args:
        grant_args.pop('GrantWrite')

    logger.info('copy file: %s/%s to %s/%s' %
                (cnf['SRC_BUCKET'], old_key_name,
                 cnf['DEST_BUCKET'], new_key_name))

    try:
        s3_client.copy_object(
            Bucket=cnf['DEST_BUCKET'],
            Key=new_key_name,
            CopySource='%s/%s' % (cnf['SRC_BUCKET'], old_key_name),
            **grant_args
        )
    except Exception as e:
        logger.exception('failed to copy file: ' + old_key_name)
        result['state'] = 'copy_object_error'

        raise MoveFileError(repr(e))


def delete_old_file(result):
    old_key_name = result['file_info']['key_name']

    logger.info('delete file: %s/%s' %
                (cnf['SRC_BUCKET'], old_key_name))

    try:
        s3_client.delete_object(
            Bucket=cnf['SRC_BUCKET'],
            Key=old_key_name,
        )
    except Exception as e:
        logger.exception('failed to delete file: ' + old_key_name)
        result['state'] = 'delete_object_error'

        raise MoveFileError(repr(e))


def move_one_file(file_info):
    result = {
        'file_info': file_info,
    }

    try:
        copy_file(result)

        if cnf['DELETE'] == True:
            delete_old_file(result)

        result['state'] = 'succeed'
        return result

    except MoveFileError as e:
        return result

    except Exception as e:
        logger.exception('got exception when move one file: ' + repr(e))

        result['state'] = 'exception'
        return result


def update_stat(result):
    MOVE_STATUS['total'] += 1
    MOVE_STATUS['total_size'] += result['file_info']['size']

    state = result['state']

    if state not in MOVE_STATUS:
        MOVE_STATUS[state] = 0

    MOVE_STATUS[state] += 1


def report_state():
    print ('iter status: total: %d, total_size: %d, marker: %s' %
           (ITER_STATUS['total'], ITER_STATUS['total_size'],
            ITER_STATUS['marker']))
    print 'move status: ' + repr(MOVE_STATUS)
    print ''


def report(sess):
    while not sess['stop']:
        report_state()
        time.sleep(cnf['REPORT_INTERVAL'])


def move_files():
    if cnf['SRC_BUCKET'] == cnf['DEST_BUCKET']:
        if (cnf['OLD_PREFIX'].startswith(cnf['NEW_PREFIX'])
                or cnf['NEW_PREFIX'].startswith(cnf['OLD_PREFIX'])):

            print (('error: OLD_PREFIX: %s, or NEW_PREFIX: %s, ' +
                    'should not starts with the other') %
                   (cnf['OLD_PREFIX'], cnf['NEW_PREFIX']))
            return

    sess = {'stop': False}

    report_th = _thread(report, (sess,))

    jobq.run(iter_file(),
             [(move_one_file, cnf['THREADS_NUM']),
              (update_stat, 1),
              ])

    sess['stop'] = True

    report_th.join()

    report_state()


def load_cli_args():
    parser = argparse.ArgumentParser(description='move file')
    parser.add_argument('cmd', type=str,
                        choices=['move_files', 'move_one_file'],
                        help='move one file by name or move files by prefix')

    parser.add_argument('--src_bucket', type=str,
                        help='the bucket which the source file in')

    parser.add_argument('--dest_bucket', type=str,
                        help='the bucket which the file will be move to')

    parser.add_argument('--old_prefix', type=str,
                        help=('set the old prefix when moving files by prefix, ' +
                              'set the source file name when moving one file'))

    parser.add_argument('--new_prefix', type=str,
                        help=('set the new prefix when moving files by prefix, ' +
                              'set the destination file name when moving one file'))

    parser.add_argument('--conf_path', type=str,
                        help='set the path of the conf path')

    args = parser.parse_args()
    return args


def load_conf(args):
    conf_path = args.conf_path or '../conf/move_file.yaml'
    conf = load_conf_from_file(conf_path)

    conf_keys = ('cmd',
                 'src_bucket',
                 'dest_bucket',
                 'old_prefix',
                 'new_prefix',
                 )

    for k in conf_keys:
        v = getattr(args, k)
        if v is not None:
            conf[k.upper()] = v

    return conf


if __name__ == "__main__":

    args = load_cli_args()
    cnf = load_conf(args)

    _mkdir(cnf['LOG_DIR'])
    logger = add_logger()

    logger.info('args={a}'.format(a=args))
    logger.info('conf={c}'.format(c=cnf))

    s3_client = boto_client()

    if cnf['CMD'] == 'move_one_file':
        file_info = {
            'key_name': cnf['OLD_PREFIX'],
        }
        print move_one_file(file_info)

    elif cnf['CMD'] == 'move_files':
        move_files()
