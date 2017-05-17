#!/usr/bin/env python2
# coding: utf-8

import time
import random
import threading

import s2http

class UploadError(Exception): pass
class DeleteError(Exception): pass
class GetError(Exception): pass

data = 't'*150*1024

hosts = ['127.0.0.1']

def upload_file(bucket, key):

    idx = random.randint(1, 10000)%len(hosts)
    http = s2http.Http(hosts[idx], 80)

    http.send_request('/'+bucket+'/'+key, 'PUT', headers = {'x-amz-acl': 'public-read', 'Content-Length': len(data), 'Host': 's2.i.qingcdn.com'} )
    http.send_body(data)

    http.finish_request()

    if http.status != 200:
        raise UploadError( 'Put File Code: ' + str(http.status) + http.read_body(1024 * 1024) )


def get_file(bucket, key):

    idx = random.randint(1, 1000)%len(hosts)
    http = s2http.Http(hosts[idx], 80)

    http.request('/'+bucket+'/'+key, 'GET')

    if http.status != 200:
        raise GetError('GET File Code: ' + str(http.status))

    http.read_body(50*1024*1024)

def delete_file(bucket, key):

    idx = random.randint(1,10000)%len(hosts)
    http = s2http.Http(hosts[idx], 80)
    #http = s2http.Http('s2.i.qingcdn.com', 80)
    http.request('/'+bucket+'/'+key, 'DELETE')

    if http.status != 204:
        raise DeleteError( 'Delete File Code: ' + str(http.status) )

def run():

    i = 0
    while True:

        #key = 'test_file_{thid}_{idx}'.format(thid=threading.currentThread().ident, idx=i)
        key = 'test_file_{thid}_{idx}'.format(thid=200, idx=i)

        start = time.time()
        #upload_file('hikcloud-test', key)
        get_file('hikcloud-test', key)

        end = time.time()

        print 'get key:{k}, start:{s}, end:{e}, time:{t}, speed:{spd} KB/sec'.format(
                                                                   k=key,
                                                                   s=start,
                                                                   e=end,
                                                                   t=end-start,
                                                                   spd=90*1024.0/(end-start)/1024)
        i += 1

        #delete_file('hikcloud-test', key)

        if i == 2000:
            break

if __name__ == "__main__":

    ths = []
    thread_num = 140
    for i in range(thread_num):
        t = threading.Thread( target=run)
        t.daemon = True
        t.start()
        ths.append(t)

    for t in ths:
        t.join()
