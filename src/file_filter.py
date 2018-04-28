# encoding:utf-8
import time

DATE_FORMAT = '%Y-%m-%d %H:%M:%S'


def is_suffix_in(key_name, suffixes):
    for suffix in suffixes:
        if key_name[0 - len(suffix):] == suffix:
            return True

    return False


def is_content_type_in(content_type, content_types):
    ct_lower = content_type.lower()

    for ct in content_types:
        if ct.lower() == ct_lower:
            return True

    return False


def check_suffix(file_object, filter_conf):
    key_name = file_object['key']

    include_suffixes = filter_conf.get('include_suffixes')

    if include_suffixes and not is_suffix_in(key_name, include_suffixes):
        return '%s suffix not in: %s' % (key_name,
                                         ', '.join(include_suffixes))

    exclude_suffixes = filter_conf.get('exclude_suffixes')
    if exclude_suffixes and is_suffix_in(key_name, exclude_suffixes):
        return '%s suffix in: %s' % (key_name,
                                     ', '.join(exclude_suffixes))

    return


def check_content_type(file_object, filter_conf):
    key_name = file_object['key']

    ct = file_object.get('content_type')
    if ct == None:
        return

    include_content_types = filter_conf.get('include_content_types')
    if include_content_types != None:
        if not is_content_type_in(ct, include_content_types):
            return ('%s content type: %s not in: %s' %
                    (key_name, ct, ', '.join(include_content_types)))

    exclude_content_types = filter_conf.get('exclude_content_types')
    if exclude_content_types != None:
        if is_content_type_in(ct, exclude_content_types):
            return ('%s content type: %s in: %s' %
                    (key_name, ct, ', '.join(exclude_content_types)))

    return


def check_last_modified(file_object, filter_conf, time_zone=None):
    time_zone = time_zone or 60 * 60 * 8

    key_name = file_object['key']

    last_modified = file_object.get('last_modified')
    if last_modified == None:
        return

    last_modified_ts = time.mktime(
        last_modified.utctimetuple()) + time_zone

    date_before = filter_conf.get('date_before')

    if date_before != None:
        date_before_ts = time.mktime(
            time.strptime(date_before, DATE_FORMAT))
        if last_modified_ts > date_before_ts:
            return ('%s last modified time: %s is not before: %s' %
                    (key_name, file_object['last_modified'], date_before))

    date_after = filter_conf.get('date_after')
    if date_after != None:
        date_after_ts = time.mktime(
            time.strptime(date_after, DATE_FORMAT))
        if last_modified_ts < date_after_ts:
            return ('%s last modified time: %s is not after: %s' %
                    (key_name, file_object['last_modified'], date_after))


def check_size(file_object, filter_conf):
    key_name = file_object['key']

    if 'size_biger_than' in filter_conf:
        if file_object['size'] < filter_conf['size_biger_than']:
            return ('%s size: %d is not biger than: %d' %
                    (key_name, file_object['size'],
                     filter_conf['size_biger_than']))

    if 'size_smaller_than' in filter_conf:
        if file_object['size'] > filter_conf['size_smaller_than']:
            return ('%s size: %d is not smaller than: %d' %
                    (key_name, file_object['size'],
                     filter_conf['size_smaller_than']))
    return


# file_object = {
    # 'key': 'test-key',
    # 'content_type': 'image/jpg',
    # 'size': 123,
    # 'last_modified': datetime.datetime.now(),
# }
def filter(file_object, filter_conf, time_zone=None):
    if type(filter_conf) != type({}):
        return

    msg = check_suffix(file_object, filter_conf)
    if msg != None:
        return msg

    msg = check_content_type(file_object, filter_conf)
    if msg != None:
        return msg

    msg = check_last_modified(file_object, filter_conf, time_zone)
    if msg != None:
        return msg

    msg = check_size(file_object, filter_conf)
    if msg != None:
        return msg

    return
