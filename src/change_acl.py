#!/usr/bin/env python2
# coding: utf-8

import copy
import datetime
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

from pykit import jobq

PERMISSIONS = ['READ', 'WRITE', 'READ_ACP', 'WRITE_ACP', 'FULL_CONTROL']
FULL_PERMISSION = ['READ', 'WRITE', 'READ_ACP', 'WRITE_ACP']
GRANTEE_TYPES = ['GROUP', 'USER_NAME', 'USER_EMAIL']

uri_to_name = {
    'http://acs.amazonaws.com/groups/global/AllUsers': 'all',
    'http://acs.amazonaws.com/groups/global/AuthenticatedUsers': 'authenticated',
    'http://acs.amazonaws.com/groups/s3/LogDelivery': 'log_delivery',
}

name_to_uri = {
    'all': 'http://acs.amazonaws.com/groups/global/AllUsers',
    'authenticated': 'http://acs.amazonaws.com/groups/global/AuthenticatedUsers',
    'log_delivery': 'http://acs.amazonaws.com/groups/s3/LogDelivery',
}

stat = {
    'total': 0,
    'failed': 0,
    'changed': 0,
    'unchanged': 0,
}


class InvalidOperationTypeError(Exception):
    pass


class InvalidACLConfigurationError(Exception):
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


def get_conf(path):
    with open(path) as f:
        conf = yaml.safe_load(f.read())

    return conf


def boto_client():
    session = boto3.session.Session()

    s3_client = session.client(
        's3',
        use_ssl=False,
        aws_access_key_id=cnf['ACCESS_KEY'],
        aws_secret_access_key=cnf['SECRET_KEY'],
        config=Config(signature_version='s3v4'),
        region_name='us-east-1',
        endpoint_url=cnf['ENDPOINT'],
    )

    return s3_client


def report(sess):
    while not sess['stop']:
        time.sleep(cnf['REPORT_INTERVAL'])

        report_str = ('total: %d  failed: %d  changed: %d  unchanged: %d' %
                      (stat['total'], stat['failed'], stat['changed'], stat['unchanged']))

        logger.info(report_str)
        print report_str


def iter_file():
    num_limit = cnf['NUM_LIMIT']

    start_marker = cnf['START_MARKER']
    end_marker = cnf['END_MARKER']

    marker = start_marker
    n = 0

    try:
        while True:
            resp = client.list_objects(
                Bucket=cnf['BUCKET_NAME'],
                Marker=marker,
                Prefix=cnf['PREFIX'],
            )

            if 'Contents' not in resp:
                break

            for content in resp['Contents']:
                if num_limit is not None and n >= num_limit:
                    return

                if end_marker is not None and content['Key'] >= end_marker:
                    return

                yield {
                    'key_name': content['Key'],
                    'etag': content['ETag'],
                    'last_modified': content['LastModified'],
                    'size': content['Size'],
                }

                n += 1
                marker = content['Key']

    except Exception as e:
        logger.error('failed to iter file: ' + traceback.format_exc())
        print 'failed to iter file: ' + repr(e)


def get_old_acl(file_info):
    resp = client.get_object_acl(
        Bucket=cnf['BUCKET_NAME'],
        Key=file_info['key_name'],
    )

    old_acl = {
        'acl': {
            'GROUP': {},
            'USER_NAME': {},
        },
    }

    old_acl['owner'] = resp['Owner']['ID']

    for grant in resp['Grants']:
        grantee_type = grant['Grantee']['Type']
        permission = grant['Permission']

        if grantee_type == 'Group':
            grantee = uri_to_name[grant['Grantee']['URI']]

            if grantee not in old_acl['acl']['GROUP']:
                old_acl['acl']['GROUP'][grantee] = []

            old_acl['acl']['GROUP'][grantee].append(permission)

        elif grantee_type == 'CanonicalUser':
            grantee = grant['Grantee']['ID']

            if grantee not in old_acl['acl']['USER_NAME']:
                old_acl['acl']['USER_NAME'][grantee] = []

            old_acl['acl']['USER_NAME'][grantee].append(permission)

    logger.info('the old acl of file: %s is: %s' %
                (file_info['key_name'], repr(old_acl)))
    return old_acl


def merge_conf_acl(old_acl):
    changed = False

    for grantee_type in GRANTEE_TYPES:
        if grantee_type not in old_acl['acl']:
            old_acl['acl'][grantee_type] = {}

        old_grants = old_acl['acl'][grantee_type]
        conf_grants = conf_acl[grantee_type]

        for grantee, permissions in conf_grants.iteritems():
            if grantee not in old_grants:
                old_grants[grantee] = permissions
                changed = True
            else:
                if 'FULL_CONTROL' in old_grants[grantee]:
                    old_grants[grantee] = copy.deepcopy(FULL_PERMISSION)

                if 'FULL_CONTROL' in permissions:
                    permissions = FULL_PERMISSION

                for permission in permissions:
                    if permission not in old_grants[grantee]:
                        old_grants[grantee].append(permission)
                        changed = True

    return changed


def remove_conf_acl_from_old_acl(old_acl):
    changed = False

    for grantee_type in GRANTEE_TYPES:
        if grantee_type not in old_acl['acl']:
            old_acl['acl'][grantee_type] = {}
            continue

        old_grants = old_acl['acl'][grantee_type]
        conf_grants = conf_acl[grantee_type]

        for grantee, permissions in conf_grants.iteritems():
            if grantee not in old_grants:
                continue

            if 'FULL_CONTROL' in old_grants[grantee]:
                old_grants[grantee] = copy.deepcopy(FULL_PERMISSION)

            if 'FULL_CONTROL' in permissions:
                permissions = FULL_PERMISSION

            remain_permissions = set(old_grants[grantee]) - set(permissions)

            if remain_permissions != set(old_grants[grantee]):
                changed = True

            old_grants[grantee] = list(remain_permissions)

    return changed


def get_conf_acl():
    acl = cnf['ACL']

    if type(acl) != type({}):
        raise InvalidACLConfigurationError(
            'the ACL in configuration file is not a dict')

    if 'GROUP' not in acl:
        acl['GROUP'] = {}

    if 'USER_NAME' not in acl:
        acl['USER_NAME'] = {}

    if 'USER_EMAIL' not in acl:
        acl['USER_EMAIL'] = {}

    for grantee_type in GRANTEE_TYPES:
        grants = acl[grantee_type]

        if type(grants) != type({}):
            raise InvalidACLConfigurationError('the configuration for %s: %s in not a dict' %
                                               (grantee_type, repr(grants)))

        for grantee, permissions in grants.iteritems():
            if permissions is None:
                grants[grantee] = []
            elif type(permissions) != type([]):
                raise InvalidACLConfigurationError('the configuration for %s %s: %s in not a list' %
                                                   (grantee_type, grantee, repr(permissions)))
            else:
                for permission in permissions:
                    if permission not in PERMISSIONS:
                        raise InvalidACLConfigurationError('the configuration for %s %s: %s is not one of %s' %
                                                           (grantee_type, grantee, repr(permission), ','.join(PERMISSIONS)))

    return acl


def build_access_control_policy(acl_to_put):
    acl = acl_to_put['acl']
    owner = acl_to_put['owner']

    policy = {
        'Grants': [],
        'Owner': {
            'DisplayName': '',
            'ID': owner,
        },
    }

    for grantee, permissions in acl['GROUP'].iteritems():
        for permission in permissions:
            policy['Grants'].append({
                'Grantee': {
                    'Type': 'Group',
                    'URI': name_to_uri[grantee],
                },
                'Permission': permission,
            })

    for grantee, permissions in acl['USER_NAME'].iteritems():
        for permission in permissions:
            policy['Grants'].append({
                'Grantee': {
                    'Type': 'CanonicalUser',
                    'ID': grantee,
                },
                'Permission': permission,
            })

    for grantee, permissions in acl['USER_EMAIL'].iteritems():
        for permission in permissions:
            policy['Grants'].append({
                'Grantee': {
                    'Type': 'AmazonCustomerByEmail',
                    'EmailAddress': grantee,
                },
                'Permission': permission,
            })

    return policy


def put_object_acl(file_info, acl_to_put):
    policy = build_access_control_policy(acl_to_put)

    logger.info('about to set acl of file: %s to %s' %
                (file_info['key_name'], repr(policy)))

    client.put_object_acl(
        AccessControlPolicy=policy,
        Bucket=cnf['BUCKET_NAME'],
        Key=file_info['key_name'],
    )


def set_acl(file_info):
    old_acl = get_old_acl(file_info)

    put_object_acl(file_info, {'acl': conf_acl, 'owner': old_acl['owner']})

    changed = merge_conf_acl(old_acl)

    return changed


def grant_acl(file_info):
    old_acl = get_old_acl(file_info)

    changed = merge_conf_acl(old_acl)

    put_object_acl(file_info, old_acl)

    return changed


def revoke_acl(file_info):
    old_acl = get_old_acl(file_info)

    changed = remove_conf_acl_from_old_acl(old_acl)

    put_object_acl(file_info, old_acl)

    return changed


def change_acl(file_info):
    result = {
        'file_info': file_info
    }

    try:
        if cnf['OPERATION_TYPE'] == 'set':
            result['changed'] = set_acl(file_info)

        elif cnf['OPERATION_TYPE'] == 'grant':
            result['changed'] = grant_acl(file_info)

        elif cnf['OPERATION_TYPE'] == 'revoke':
            result['changed'] = revoke_acl(file_info)

        else:
            raise InvalidOperationTypeError('invalid operation type: ' +
                                            repr(cnf['OPERATION_TYPE']))
        return result

    except Exception as e:
        logger.error('failed to change acl of file: %s, %s' %
                     (repr(file_info), traceback.format_exc()))
        print 'failed to change acl of file: %s, %s' % (repr(file_info), repr(e))
        result['error'] = True
        return result


def update_stat(result):
    stat['total'] += 1

    if result.get('error') == True:
        stat['failed'] += 1
        return

    if result.get('changed') == True:
        stat['changed'] += 1
    else:
        stat['unchanged'] += 1


def run_one_turn():

    logger.warn('one turn started')
    print 'one turn started'

    report_sess = {'stop': False}

    stat['total'] = 0
    stat['failed'] = 0
    stat['changed'] = 0
    stat['unchanged'] = 0

    report_th = _thread(report, (report_sess,))

    jobq.run(iter_file(),
             [(change_acl, cnf['THREADS_NUM_FOR_CHANGE_ACL']),
              (update_stat, 1),
              ])

    report_sess['stop'] = True

    report_th.join()


def run_forever():

    while True:
        run_one_turn()

        time_to_sleep = 60 * 60 * 2
        print 'sleep %d seconds before running next turn' % time_to_sleep

        time.sleep(time_to_sleep)


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


def add_logger():

    log_file = os.path.join(cnf['LOG_DIR'], 'change-acl-log-for-' +
                            cnf['BUCKET_NAME'] + '.log')

    log = logging.getLogger()
    log.setLevel(logging.INFO)

    file_handler = logging.FileHandler(log_file)
    formatter = logging.Formatter('[%(asctime)s, %(levelname)s] %(message)s')

    file_handler.setFormatter(formatter)

    log.addHandler(file_handler)

    return log


def confirm():
    print 'Operation: ' + cnf['OPERATION_TYPE']
    print 'ACL:'
    for grantee_type in GRANTEE_TYPES:
        for grantee, permissions in conf_acl[grantee_type].iteritems():
            if len(permissions) > 0:
                print '    %s: %s' % (grantee, ','.join(permissions))

    print 'input "y" to confirm'
    answer = raw_input()

    if answer != 'y':
        print 'your input: %s is not "y", exit' % answer
        sys.exit()

    print 'confirmed, continue'


if __name__ == "__main__":

    opts, _ = getopt.getopt(sys.argv[1:], '', ['conf=', ])
    opts = dict(opts)

    if opts.get('--conf') is None:
        conf_path = '../conf/change_acl.yaml'
    else:
        conf_path = opts['--conf']

    cnf = get_conf(conf_path)

    conf_acl = get_conf_acl()

    confirm()

    client = boto_client()

    _mkdir(cnf['LOG_DIR'])

    logger = add_logger()

    if cnf['RUN_FOREVER']:
        run_forever()
    else:
        run_one_turn()
