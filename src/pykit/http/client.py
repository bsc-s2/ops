#!/usr/bin/env python2
# coding: utf-8

import errno
import logging
import select
import socket

logger = logging.getLogger(__name__)


class HttpError(Exception):
    pass


class LineTooLongError(HttpError):
    pass


class ChunkedSizeError(HttpError):
    pass


class NotConnectedError(HttpError):
    pass


class ResponseNotReadyError(HttpError):
    pass


class HeadersError(HttpError):
    pass


class BadStatusLineError(HttpError):
    pass


MAX_LINE_LENGTH = 65536
LINE_RECV_LENGTH = 1024 * 4


class Client(object):

    def __init__(self, host, port, timeout=60):

        self.host = host
        self.port = port
        self.timeout = timeout
        self.sock = None
        self.chunked = False
        self.chunk_left = None
        self.content_length = None
        self.has_read = 0
        self.status = None
        self.headers = {}
        self.recv_iter = None

    def __del__(self):

        self._close()

    def request(self, uri, method='GET', headers={}):

        self.send_request(uri, method=method, headers=headers)

        self.read_response()

    def send_request(self, uri, method='GET', headers={}):

        self._reset_request()
        self.method = method

        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.settimeout(self.timeout)
        self.sock.connect((self.host, self.port))

        bufs = ['{method} {uri} HTTP/1.1'.format(method=method, uri=uri), ]

        headers = headers or {}
        if 'Host' not in headers and 'host' not in headers:
            headers['Host'] = self.host

        for k, v in headers.items():
            bufs.append('%s: %s' % (k, v))

        bufs.extend(['', ''])

        self.sock.sendall('\r\n'.join(bufs))

    def send_body(self, body):

        if self.sock is None:
            raise NotConnectedError('socket object is None')

        self.sock.sendall(body)

    def read_status(self):

        if self.status is not None or self.sock is None:
            raise ResponseNotReadyError('response is unavailable')

        self.recv_iter = _recv_loop(self.sock, self.timeout)
        self.recv_iter.next()

        # read until we get a non-100 response
        while True:

            status = self._get_response_status()
            if status >= 200:
                break

            # skip the header from the 100 response
            while True:
                skip = self._readline()
                if skip.strip() == '':
                    break

        self.status = status
        return status

    def read_headers(self):

        while True:

            line = self._readline()
            if line == '':
                break

            kv = line.strip().split(':', 1)
            if len(kv) < 2:
                raise HeadersError('invalid headers param line:%s' % (line))
            self.headers[kv[0].lower()] = kv[1].strip()

        if self.status in (204, 304) or self.method == 'HEAD':
            self.content_length = 0
            return self.headers

        code = self.headers.get('transfer-encoding', '')
        if code.lower() == 'chunked':
            self.chunked = True
            return self.headers

        length = self.headers.get('content-length', '0')

        try:
            self.content_length = int(length)
        except ValueError as e:
            logger.error(
                repr(e) + ' while get content-length length:{l}'.format(l=length))
            raise HeadersError('invalid content-length')

        return self.headers

    def read_response(self):

        self.read_status()
        self.read_headers()

        return self.status, self.headers

    def read_body(self, size):

        if size is not None and size <= 0:
            return ''

        if self.chunked:
            buf = self._read_chunked(size)
            self.has_read += len(buf)
            return buf

        if size is None:
            size = self.content_length - self.has_read
        else:
            size = min(size, self.content_length - self.has_read)

        if size <= 0:
            return ''

        buf = self._read(size)
        self.has_read += size

        return buf

    def _close(self):

        if self.recv_iter is not None:
            try:
                self.recv_iter.close()
            except Exception as e:
                logger.Exception(repr(e) + ' while close recv_iter')

        self.recv_iter = None

        if self.sock is not None:
            try:
                self.sock.close()
            except Exception as e:
                logger.Exception(repr(e) + ' while close sock')

        self.sock = None

    def _reset_request(self):

        self._close()
        self.chunked = False
        self.chunk_left = None
        self.content_length = None
        self.has_read = 0
        self.status = None
        self.headers = {}

    def _read(self, size):
        return self.recv_iter.send(('block', size))

    def _readline(self):
        return self.recv_iter.send(('line', None))

    def _get_response_status(self):

        line = self._readline()

        vals = line.split(None, 2)
        if len(vals) < 2:
            raise BadStatusLineError('invalid status line:{l}'.format(l=line))

        ver, status = vals[0], vals[1]

        try:
            status = int(status)
        except ValueError as e:
            logger.error(repr(e) + ' while get response status')
            raise BadStatusLineError('status is not int:{l}'.format(l=line))

        if not ver.startswith('HTTP/') or status < 100 or status > 999:
            raise BadStatusLineError('invalid status line:{l}'.format(l=line))

        return status

    def _get_chunk_size(self):

        line = self._readline()

        i = line.find(';')
        if i >= 0:
            # strip chunk-extensions
            line = line[:i]

        try:
            chunk_size = int(line, 16)
        except ValueError as e:
            logger.error(
                repr(e) + ' while get chunk size line:{l}'.format(l=line))
            raise ChunkedSizeError('invalid chunk size')

        return chunk_size

    def _read_chunked(self, size):

        buf = []

        if self.chunk_left == 0:
            return ''

        while size is None or size > 0:

            if self.chunk_left is None:
                self.chunk_left = self._get_chunk_size()

            if self.chunk_left == 0:
                break

            if size is not None:
                read_size = min(size, self.chunk_left)
                size -= read_size
            else:
                read_size = self.chunk_left

            buf.append(self._read(read_size))
            self.chunk_left -= read_size

            if self.chunk_left == 0:
                self._read(len('\r\n'))
                self.chunk_left = None

        if self.chunk_left == 0:

            while True:
                line = self._readline()
                if line == '':
                    break

        return ''.join(buf)


def _recv_loop(sock, timeout):

    bufs = ['']
    mode, size = yield

    while True:

        if mode == 'line':
            buf = bufs[0]
            if '\r\n' in buf:
                rst, buf = buf.split('\r\n', 1)
                bufs[0] = buf
                mode, size = yield rst
                continue
            else:
                if len(buf) >= MAX_LINE_LENGTH:
                    raise LineTooLongError(
                        'line length greater than max_len:{l}'.format(l=len(buf)))
                else:
                    buf += _recv(sock, timeout, LINE_RECV_LENGTH)
                    bufs[0] = buf
                    continue
        else:
            total = len(bufs[0])
            while total < size:
                bufs.append(_recv(sock, timeout, size - total))
                total += len(bufs[-1])

            rst = ''.join(bufs)
            if size < len(rst):
                bufs = [rst[size:]]
                rst = rst[:size]
            else:
                bufs = ['']
            mode, size = yield rst


def _recv(sock, timeout, size):

    buf = ''
    for _ in range(2):
        try:
            buf = sock.recv(size)
            break
        except socket.error as e:
            if len(e.args) <= 0 or e.args[0] != errno.EAGAIN:
                raise

            evin, evout, everr = select.select(
                [sock.fileno()], [], [], timeout)

            if len(evin) <= 0:
                raise socket.timeout(
                    '{second}s timeout while recv'.format(second=timeout))

    if len(buf) <= 0:
        raise socket.error('got empty when recv {l} bytes'.format(l=size))

    return buf
