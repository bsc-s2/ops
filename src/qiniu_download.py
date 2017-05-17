#!/usr/bin/env python2
#-*- coding:utf-8 -*-
#
# AUTHOR = "heqingpan"
# AUTHOR_EMAIL = "heqingpan@126.com"
# URL = "http://git.oschina.net/hqp/qiniu_sync"

# pip install qiniu

import os
import time
import sys
import urllib2

from qiniu import Auth
from qiniu import BucketManager
import traceback

from pykit import jobq


access_key    = '234dsdgf234dsdgf234dsdgf'
secret_key    = '123qwe123qwe123qwe123qwe123qwe'
bucket_name   = 'examplebucket'
bucket_domain = 'bucketdomin.com'

q = Auth(access_key, secret_key)
bucket = BucketManager(q)
basedir = os.path.realpath(os.path.dirname(__file__))
# 同步目录
# basedir=""
filename = __file__
ignore_paths = [filename, "{0}c".format(filename)]
ignore_names = [".DS_Store", ".git", ".gitignore"]
charset = "utf8"
diff_time = 2 * 60


def list_all(bucket_name, bucket=None, prefix="", limit=100):

    if bucket is None:
        bucket = BucketManager(q)

    marker = None
    eof = False

    while eof is False:

        try:
            ret, eof, info = bucket.list(bucket_name, prefix=prefix,
                                         marker=marker, limit=limit)
            # print ret, eof, info
        except Exception as e:
            print repr(e) + ' while list from marker={m}'.format(m=marker)
            time.sleep(1)
            continue

        marker = ret.get('marker', None)

        for item in ret['items']:
            # print 'got:', item
            yield item


def down_file(item, basedir="", is_private=1, expires=3600):

    print item

    key = item['key']

    if isinstance(key, unicode):
        key = key.encode(charset)

    url = 'http://%s/%s' % (bucket_domain, key)

    if is_private:
        url = q.private_download_url(url, expires=expires)

    c = urllib2.urlopen(url)

    fpath = key.replace("/", os.sep)
    savepath = os.path.join(basedir, fpath)

    dir_ = os.path.dirname(savepath)

    if not os.path.isdir(dir_):
        os.makedirs(dir_)

    if os.path.isfile(savepath):
        os.remove(savepath)

    f = file(savepath, 'wb')
    try:

        written = 0
        while True:
            buf = c.read(1024*1024)
            if buf == '':
                break
            f.write(buf)

            written += len(buf)

            print 'write: {p} {l} / {s}  '.format(p=100*written / item['fsize'], l=written, s=item['fsize'])

    except Exception as e:
        print repr(e)
        pass
    finally:
        f.close()


def down_all(prefix=""):
    it = list_all(bucket_name, bucket, prefix=prefix)
    def _dd(item):
        print item
        down_file(item, basedir=basedir)
    jobq.run(it, [
            (_dd, 5),
    ])

    # for item in list_all(bucket_name, bucket, prefix=prefix):

    #     key = item['key']

    #     for ii in range(3):
    #         try:
    #             down_file(item, basedir=basedir)
    #             print "down:\t" + key
    #         except Exception as e:
    #             print repr(e) + ' while downlod {k}'.format(k=key)
    #             print traceback.format_exc()

    print "down end"


def main():

    if len(sys.argv) > 1:

        if sys.argv[1] == "down":

            prefix = len(sys.argv) > 2 and sys.argv[2] or ""
            down_all(prefix=prefix)
            return

if __name__ == "__main__":
    main()
