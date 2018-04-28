#!/usr/bin/env python2
# coding:utf-8

import argparse
import math
import time

import yaml

import util


def check_size(file_size, size_info):
    if file_size == 0:
        size_info['size_zero_n'] += 1
        return

    index = int(math.log10(file_size))
    size_info['log10'][index] += 1


def check_key_type(key_name, key_types):
    ty = str(type(key_name))

    key_types[ty] = key_types.get(ty, 0) + 1


def check_extension(key_name, extensions):
    ext = key_name.split('.')[-1]

    if ext == key_name:
        ext = ''

    extensions[ext] = extensions.get(ext, 0) + 1


def check_upload_time(ts_now, last_modified, upload_time):
    ts_upload = time.mktime(last_modified.utctimetuple())
    ts_upload += cnf.get('TIME_ZOE', 60 * 60 * 8)

    ts_diff = ts_now - ts_upload

    if ts_diff < 60:
        upload_time['in_minute'] += 1
        return

    if ts_diff < 60 * 60:
        upload_time['in_hour'] += 1
        return

    if ts_diff < 60 * 60 * 24:
        upload_time['in_day'] += 1
        return

    if ts_diff < 60 * 60 * 24 * 7:
        upload_time['in_week'] += 1
        return

    if ts_diff < 60 * 60 * 24 * 30:
        upload_time['in_month'] += 1
        return

    if ts_diff < 60 * 60 * 24 * 365:
        upload_time['in_year'] += 1
        return

    upload_time['year_ago'] += 1
    return


def show_size(size_info):
    print 'distribution of size:'
    print '    number of zero size file: %d' % size_info['size_zero_n']
    log10 = size_info['log10']
    for i in range(len(log10)):
        print '    %s : %d' % (util.format_size(10 ** i), log10[i])


def show_key_types(key_types):
    print 'type of key:'
    for k, v in key_types.iteritems():
        print '    %s: %d' % (k, v)


def show_extensions(extensions):
    print 'extension of files:'
    for k in extensions.keys()[:20]:
        print '    %s: %d' % (k, extensions[k])
    if len(extensions) > 20:
        print '...'


def show_upload_time(upload_time):
    print 'upload time of files:'
    for k in (
        'in_minute',
        'in_hour',
        'in_day',
        'in_week',
        'in_month',
        'in_year',
        'year_ago',
    ):
        print '    %s: %d' % (k, upload_time[k])


def count():
    total_n = 0
    total_size = 0

    dir_n = 0
    bad_dir_n = 0
    bad_dir_size = 0

    size_info = {
        'size_zero_n': 0,
        'log10': [0] * 15,
    }

    key_types = {}
    extensions = {}
    upload_time = {
        'in_minute': 0,
        'in_hour': 0,
        'in_day': 0,
        'in_week': 0,
        'in_month': 0,
        'in_year': 0,
        'year_ago': 0,
    }

    ts_now = time.time()

    for file_object in util.iter_file(s3_client, cnf['BUCKET_NAME'],
                                      prefix=cnf['PREFIX']):
        last_modified = file_object['LastModified']
        key_name = file_object['Key']
        file_size = file_object['Size']

        total_n += 1
        total_size += file_size

        if total_n % 1000 == 0:
            print 'listed %d files' % total_n

        if key_name.endswith('/'):
            dir_n + 1
            if file_size != 0:
                bad_dir_n += 1
                bad_dir_size += file_size

        check_size(file_size, size_info)
        check_key_type(key_name, key_types)
        check_extension(key_name, extensions)
        check_upload_time(ts_now, last_modified, upload_time)

    print 'listed %d files' % total_n

    print ''
    print 'total file number: %s' % util.format_number(total_n)
    print 'total size: %s' % util.format_size(total_size)
    print 'number of files that endswith /: %d' % dir_n
    print 'number of files that endswith / and size is not zero: %d' % bad_dir_n
    print 'total size of files that endswith / and size is not zero: %s' % util.format_size(bad_dir_size)

    print ''
    show_size(size_info)

    print ''
    show_key_types(key_types)

    print ''
    show_extensions(extensions)

    print ''
    show_upload_time(upload_time)


def change_domain(url):
    acc_domain = cnf.get('ACCELERATE_DOMAIN')
    if acc_domain == None:
        return url

    use_virtual_host = cnf.get('VIRTUAL_HOST', True)

    if use_virtual_host:
        to_replace = '%s/%s' % (cnf['ENDPOINT'], cnf['BUCKET_NAME'])
    else:
        to_replace = cnf['ENDPOINT']

    return url.replace(to_replace, cnf['ACCELERATE_DOMAIN'])


def list_files():
    list_file_name = 'file_list_of_%s_%s.txt' % (cnf['BUCKET_NAME'],
                                                 cnf['PREFIX'])
    f = open(list_file_name, 'wb')

    total_n = 0
    for file_object in util.iter_file(s3_client, cnf['BUCKET_NAME'],
                                      prefix=cnf['PREFIX']):
        key_name = file_object['Key']

        key_name = util.to_unicode(key_name)

        url = s3_client.generate_presigned_url(
            'get_object',
            Params={
                'Bucket': cnf['BUCKET_NAME'],
                'Key': key_name,
            },
            ExpiresIn=cnf.get('EXPIRESIN', 60 * 60),
        )

        key_name = key_name.encode('utf-8')
        url = url.encode('utf-8')

        url = change_domain(url)

        line = '%s\n' % key_name
        # line = '%s %s\n' % (key_name, url)

        f.write(line)
        total_n += 1

    f.close()
    print 'wrote %d lines to file: %s' % (total_n, list_file_name)


def load_cli_args():
    parser = argparse.ArgumentParser(description='count and statistics')
    parser.add_argument('cmd', type=str,
                        choices=['count', 'list'],
                        help='count number, size or list out to a file')

    parser.add_argument('--access_key', type=str,
                        help='set user access key')

    parser.add_argument('--secret_key', type=str,
                        help='set user secret key')

    parser.add_argument('--bucket_name', type=str,
                        help='the bucket name intrested')

    parser.add_argument('--prefix', type=str,
                        help='the prefix to use')

    parser.add_argument('--conf_path', type=str,
                        help='set the path of the conf path')

    args = parser.parse_args()
    return args


def load_conf_from_file(path):
    with open(path) as f:
        conf = yaml.safe_load(f.read())

    return conf


def load_conf(args):
    conf_path = args.conf_path or '../conf/count.yaml'
    conf = load_conf_from_file(conf_path)

    conf_keys = ('cmd',
                 'access_key',
                 'secret_key',
                 'bucket_name',
                 'prefix',
                 )

    for k in conf_keys:
        v = getattr(args, k)
        if v is not None:
            conf[k.upper()] = v

    return conf


if __name__ == "__main__":

    cli_args = load_cli_args()
    cnf = load_conf(cli_args)

    s3_client = util.get_boto_client(
        cnf['ACCESS_KEY'],
        cnf['SECRET_KEY'],
        endpoint=cnf['ENDPOINT'],
    )

    if cnf['CMD'] == 'count':
        count()
    elif cnf['CMD'] == 'list':
        list_files()
