#!/usr/bin/env python2
# coding:utf-8


import logging

import util

logger = logging.getLogger(__name__)


def get_s3_file_info(s3_client, bucket_name, s3_key):
    resp = s3_client.head_object(
        Bucket=bucket_name,
        Key=util.to_unicode(s3_key),
    )

    s3_file_info = {
        'size': resp['ContentLength'],
        'content_type': resp['ContentType'],
        'meta': resp['Metadata'],
        'content_md5': resp['ETag'].lower().strip('"'),
    }

    return s3_file_info


def check_if_need_sync(conf, file_info, bucket_name, s3_key, s3_client, log_prefix, status):
    if conf.get('CHECK_EXIST') is False:
        return True

    try:
        s3_file_info = get_s3_file_info(s3_client, bucket_name, s3_key)

    except Exception as e:
        if hasattr(e, 'message') and 'Not Found' in e.message:
            logger.info('%s file: %s not found in s3, need to sync' %
                        (log_prefix, s3_key))
            return True
        else:
            logger.exception(('%s faied to get s3 file info when check ' +
                              'if need to sync %s: %s') %
                             (log_prefix, s3_key, repr(e)))
            status['s3_get_error'] = status.get('s3_get_error', 0) + 1
            return False

    status['exist'] = status.get('exist', 0) + 1

    if 'size' in file_info and file_info['size'] != s3_file_info['size']:
        status['size_not_equal'] = status.get('size_not_equal', 0) + 1
        logger.info(('%s need to override file: %s, because size not equal, ' +
                     'size: %d, s3_size: %d') %
                    (log_prefix, s3_key, file_info['size'], s3_file_info['size']))
        return True

    md5 = file_info.get('md5', '')
    if s3_file_info['content_md5'].lower() == md5.lower():
        status['md5_equal'] = status.get('md5_equal', 0) + 1
        return False

    if conf.get('OVERRIDE') is True:
        status['override'] = status.get('override', 0) + 1
        return True

    return False


def compare_file_info(s3_key, file_info, s3_file_info, log_prefix, status):
    if 'size' in file_info and file_info['size'] != s3_file_info['size']:
        logger.error(('%s compare failed for key: %s, size: %d, ' +
                      's3 file size: %d') %
                     (log_prefix, s3_key, file_info['size'],
                      s3_file_info['size']))

        status['compare_size_error'] = status.get('compare_size_error', 0) + 1
        return False

    content_type = file_info.get('content_type')
    if (content_type is not None and
            content_type.lower() != s3_file_info['content_type'].lower()):
        logger.error(('%s compare failed for key: %s, content type: %s, ' +
                      's3 content type: %s') %
                     (log_prefix, s3_key, file_info['content_type'],
                      s3_file_info['content_type']))

        status['compare_type_error'] = status.get('compare_type_error', 0) + 1
        return False

    meta = file_info.get('meta') or {}

    for k, v in meta.iteritems():
        if k not in s3_file_info['meta'] or v != s3_file_info['meta'][k]:
            logger.error(('%s compare meta: %s failed for key: %s, ' +
                          'file meta: %s, s3 meta: %s') %
                         (log_prefix, s3_key, k, repr(v),
                          repr(s3_file_info['meta'].get(k))))

            status['compare_meta_error'] = status.get(
                'compare_meta_error', 0) + 1
            return False

    if 'md5' in file_info:
        if file_info['md5'].lower() != s3_file_info['content_md5'].lower():
            logger.error(('%s compare failed for key: %s, md5: %s, ' +
                          's3 md5: %s') %
                         (log_prefix, s3_key, file_info['md5'],
                          s3_file_info['content_md5']))

            status['compare_md5_error'] = status.get(
                'compare_md5_error', 0) + 1
            return False

    return True


def compare_file(bucket_name, s3_key, file_info, s3_client, log_prefix, status):
    try:
        s3_file_info = get_s3_file_info(s3_client, bucket_name, s3_key)

    except Exception as e:
        status['compare_error'] = status.get('compare_error', 0) + 1

        logger.exception('%s got exception when get s3 file info %s: %s' %
                         (log_prefix, s3_key, repr(e)))
        return False

    succeed = compare_file_info(s3_key, file_info, s3_file_info,
                                log_prefix, status)
    if not succeed:
        return False

    return True
