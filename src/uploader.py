#!/usr/bin/env python2
# coding:utf-8

import math
import xml

from pykit import http


class ReadBodyError(Exception):
    pass


class ResponseStatusError(Exception):
    pass


def pipe_data(request, http_read, callback, pipe_chunk_size, token_bucket):
    http_s3 = http.Client(request['headers']['Host'], 80, timeout=60 * 60)
    http_s3.send_request(request['uri'],
                         method=request['verb'],
                         headers=request['headers'])

    file_size = request['headers']['Content-Length']

    piped_size = 0
    while True:
        tokens = min(file_size - piped_size, pipe_chunk_size)
        token_bucket.get_tokens(tokens)

        buf = http_read.read_body(tokens)

        if len(buf) != tokens:
            message = ('faield to pipe data to: %s, buf len: %d, tokens: %d'
                       % (request['uri'], len(buf), tokens))
            raise ReadBodyError(message)

        http_s3.send_body(buf)

        piped_size += len(buf)

        callback({'piped_size': piped_size, 'tokens': tokens})

        if piped_size == file_size:
            break

    http_s3.read_response()

    if http_s3.status != 200:
        message = ('got invalid resp when pipe to: %s, status: %s, resp: %s'
                   % (request['uri'], http_s3.status, http_s3.read_body(1024)))
        raise ResponseStatusError(message)

    resp_headers = http_s3.headers
    resp_body = http_s3.read_body(1024 * 10)

    return (resp_headers, resp_body)


def s3_upload_single_part(http_read, s3_key, file_size, content_type,
                          s3_signer, token_bucket, bucket_name, endpoint,
                          callback, acl, pipe_chunk_size, extra_headers):
    s3_request = {
        'verb': 'PUT',
        'uri': '/%s/%s' % (bucket_name, s3_key),
        'headers': {
            'Host': endpoint,
            'Content-Length': file_size,
            'Content-Type': content_type,
            'X-Amz-Acl': acl,
        },
    }

    if extra_headers is not None:
        s3_request['headers'].update(extra_headers)

    s3_signer.add_auth(s3_request, sign_payload=False)

    return pipe_data(s3_request, http_read, callback, pipe_chunk_size, token_bucket)


def get_upload_id(body):
    tree = xml.etree.ElementTree.fromstring(body)
    for child in tree.getchildren():
        if child.tag.endswith('UploadId'):
            return child.text


def make_complete_body(parts_etag):
    xml_parts = []
    for i in range(len(parts_etag)):
        xml_parts.append(
            '<Part><PartNumber>%d</PartNumber><ETag>&quot;%s&quot;</ETag></Part>' %
            (i + 1, parts_etag[i]))

    return ('<CompleteMultipartUpload>%s</CompleteMultipartUpload>' %
            ''.join(xml_parts))


def s3_upload_multi_part(http_read, s3_key, file_size, multipart_threshold,
                         content_type, s3_signer, token_bucket, bucket_name,
                         endpoint, callback, acl, pipe_chunk_size, extra_headers):
    s3_request = {
        'verb': 'POST',
        'uri': '/%s/%s' % (bucket_name, s3_key),
        'args': {
            'uploads': True,
        },
        'headers': {
            'Host': endpoint,
            'Content-Length': file_size,
            'Content-Type': content_type,
            'X-Amz-Acl': acl,
        },
    }

    if extra_headers is not None:
        s3_request['headers'].update(extra_headers)

    s3_signer.add_auth(s3_request, sign_payload=False)

    http_s3 = http.Client(s3_request['headers']['Host'], 80, timeout=60 * 60)
    http_s3.send_request(s3_request['uri'],
                         method=s3_request['verb'],
                         headers=s3_request['headers'])

    http_s3.read_response()
    resp_body = http_s3.read_body(1024 * 10)

    if http_s3.status != 200:
        message = ('got invalid resp when init multipart: %s, status: %s, resp: %s'
                   % (s3_request['uri'], http_s3.status, resp_body))
        raise ResponseStatusError(message)

    upload_id = get_upload_id(resp_body)

    nr_part = int(math.ceil(float(file_size) / multipart_threshold))
    curr_part = 1
    parts_etag = []

    def multipart_callback(arg):
        arg['piped_size'] += multipart_threshold * (curr_part - 1)
        callback(arg)

    for i in range(nr_part):
        curr_part = i + 1

        if curr_part == nr_part:
            part_size = file_size % multipart_threshold
        else:
            part_size = multipart_threshold

        s3_request = {
            'verb': 'PUT',
            'uri': '/%s/%s' % (bucket_name, s3_key),
            'args': {
                'partNumber': str(curr_part),
                'uploadId': upload_id,
            },
            'headers': {
                'Host': endpoint,
                'Content-Length': part_size,
            },
        }
        s3_signer.add_auth(s3_request, sign_payload=False)

        resp_headers, _ = pipe_data(s3_request, http_read, multipart_callback,
                                    pipe_chunk_size, token_bucket)

        parts_etag.append(resp_headers['etag'].strip('"'))

    body = make_complete_body(parts_etag)
    s3_request = {
        'verb': 'POST',
        'uri': '/%s/%s' % (bucket_name, s3_key),
        'args': {
            'uploadId': upload_id,
        },
        'headers': {
            'Host': endpoint,
            'Content-Length': len(body),
        },
    }
    s3_signer.add_auth(s3_request, sign_payload=False)

    http_s3 = http.Client(s3_request['headers']['Host'], 80, timeout=60 * 60)
    http_s3.send_request(s3_request['uri'],
                         method=s3_request['verb'],
                         headers=s3_request['headers'])

    http_s3.send_body(body)
    http_s3.read_response()

    if http_s3.status != 200:
        message = ('got invalid resp when complete multipart: %s, status: %s, resp: %s'
                   % (s3_request['uri'], http_s3.status, http_s3.read_body(1024)))
        raise ResponseStatusError(message)

    return True


def s3_upload(http_read, s3_key, file_size, content_type, token_bucket,
              s3_signer, bucket_name, endpoint, callback, acl='private',
              pipe_chunk_size=1024*1024*10, extra_headers=None,
              multipart_threshold=1024*1024*1024*10):

    if file_size < multipart_threshold:
        return s3_upload_single_part(http_read, s3_key, file_size, content_type,
                                     s3_signer, token_bucket, bucket_name, endpoint,
                                     callback, acl, pipe_chunk_size, extra_headers)
    else:
        return s3_upload_multi_part(http_read, s3_key, file_size, multipart_threshold,
                                    content_type,
                                    s3_signer, token_bucket, bucket_name, endpoint,
                                    callback, acl, pipe_chunk_size, extra_headers)
