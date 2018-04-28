#!/usr/bin/env python2
# coding:utf-8

import errno
import logging
import hashlib
import os
import magic
import urllib
from urlparse import urlparse
from datetime import datetime

import boto3
from botocore.client import Config

mime = magic.Magic(mime=True)

ENCODINGS = [
    'utf-8',
    'gbk',
    'cp1252',
]

DATE_FORMAT = '%Y-%m-%d %H:%M:%S'

KB = 1204.0 ** 1
MB = 1024.0 ** 2
GB = 1024.0 ** 3
TB = 1024.0 ** 4

class DecodeError(Exception):
    pass



def str_to_hex(str):
    str_buffer = buffer(str)
    r = []
    for b in str_buffer:
        r.append(hex(ord(b)))

    return ' '.join(r)


def to_unicode(str, encodings=None):
    if isinstance(str, unicode):
        return str

    to_try = encodings or []
    to_try += ENCODINGS

    for encoding_type in to_try:
        try:
            return str.decode(encoding_type)
        except UnicodeDecodeError:
            if encoding_type == to_try[-1]:
                raise DecodeError('can not decode: %s' % str_to_hex(str))


def to_utf8(str, encodings=None):
    u_str = to_unicode(str, encodings)
    return u_str.encode('utf-8')


def parse_url(url):
    parse_result = urlparse(url)

    host, _, port = parse_result.netloc.partition(':')
    if len(port) > 0:
        port = int(port)
    else:
        port = 80

    uri = parse_result.path

    if len(parse_result.query) > 0:
        uri += '?' + parse_result.query

    return (host, port, uri)


def get_boto_client(access_key, secret_key, **argkv):
    signature_version = argkv.get('signature_version', 's3')
    endpoint = argkv.get('endpoint', 's2.i.qingcdn.com')

    client = boto3.client(
        's3',
        use_ssl=False,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        config=Config(signature_version=signature_version),
        region_name='us-east-1',
        endpoint_url='http://' + endpoint,
    )

    return client


def format_size(size):
    in_kb = size / KB
    in_mb = size / MB
    in_gb = size / GB
    in_tb = size / TB

    s = ('%d B (%.3f KB, %.3f MB, %.3f GB, %.3f TB)' %
         (size, in_kb, in_mb, in_gb, in_tb))

    return s


def format_number(n):
    n_str = str(n)

    chunk_list = []

    while True:
        chunk_list.insert(0, n_str[-3:])
        n_str = n_str[:-3]

        if n_str == '':
            break

    return '%s (%s)' % (str(n), ','.join(chunk_list))


def iter_file(s3_client, bucket_name, prefix='', marker=''):
    while True:
        resp = s3_client.list_objects(
            Bucket=bucket_name,
            Prefix=prefix,
            Marker=marker,
        )

        if 'Contents' not in resp:
            break

        for content in resp['Contents']:
            yield content

        marker = resp['Contents'][-1]['Key']


def add_logger(log_dir, log_name):
    format = ('[%(asctime)s,%(process)d-%(thread)d,%(filename)s,' +
              '%(lineno)d,%(levelname)s] %(message)s')
    formatter = logging.Formatter(format)

    log_file = os.path.join(log_dir, log_name)
    file_handler = logging.FileHandler(log_file)
    file_handler.setFormatter(formatter)

    log = logging.getLogger()
    log.setLevel(logging.INFO)
    log.addHandler(file_handler)

    return log


def get_log_prefix(str):
    str = to_utf8(str)
    log_prefix = '###%s>>:' % hashlib.sha1(str).hexdigest()[:20]
    return log_prefix


def is_visible_dir(dir_name):
    if dir_name.startswith('.'):
        return False

    return True


def is_visible_file(file_name):
    if file_name.startswith('.'):
        return False

    return True


def iter_dir(dir_name):
    dir_name = dir_name.rstrip('/')
    q = []
    q.append(dir_name)

    while True:
        if len(q) < 1:
            break
        dir = q.pop(0)

        files = os.listdir(dir)

        for f in files:
            if not is_visible_dir(f):
                continue

            sub_dir = dir + '/' + f

            if os.path.isdir(sub_dir):
                q.append(sub_dir)

        yield dir


def get_dir_file_list(dir_name):
    dir_name = dir_name.rstrip('/')

    files = []
    for f in os.listdir(dir_name):
        if not is_visible_file(f):
            continue

        f_path = dir_name + '/' + f

        if os.path.isfile(f_path):
            files.append(f_path)

    return files


def get_file_mime_type(file_path):
    return mime.from_file(file_path).lower()
