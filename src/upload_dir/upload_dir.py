#!/usr/bin/env python2
# coding: utf-8

import os
import re
import sys
import threading
import time
import types
import logging
import yaml
import hashlib
import chardet
import s2apiclient

if sys.version_info[0] == 2:
    import Queue
else:
    import queue as Queue

log_path = 'upload_dir.log'
log_format = '%(asctime)s:%(levelname)s:%(funcName)s:%(lineno)s:%(message)s'
level = logging.ERROR
logging.basicConfig(format=log_format,
                    level=level,
                    stream=open(log_path, 'a'))
logger = logging.getLogger(__name__)
domain = 'ss.bscstorage.com'
ak = ''
sk = ''
MB = 1024 * 1024


def encode_utf8(s):
    return s.encode('utf-8') if type(s) is unicode else s


class EmptyRst(object):
    pass


class Finish(object):
    pass


class JobWorkerError(Exception):
    pass


class JobWorkerNotFound(JobWorkerError):
    pass


class WorkerGroup(object):

    def __init__(self, index, worker, n_thread,
                 input_queue,
                 dispatcher,
                 probe, keep_order):

        self.index = index
        self.worker = worker
        self.n_thread = n_thread
        self.input_queue = input_queue
        self.output_queue = _make_q()
        self.dispatcher = dispatcher
        self.probe = probe
        self.keep_order = keep_order

        self.threads = {}

        # When exiting, it is not allowed to change thread number.
        # Because we need to send a `Finish` to each thread.
        self.exiting = False

        self.running = True

        # left close, right open: 0 running, n not.
        # Indicate what thread should have been running.
        self.running_index_range = [0, self.n_thread]

        # protect reading/writing worker_group info
        self.worker_group_lock = threading.RLock()

        # dispatcher mode implies keep_order
        if self.dispatcher is not None or self.keep_order:
            need_coordinator = True
        else:
            need_coordinator = False

        # Coordinator guarantees worker-group level first-in first-out, by put
        # output-queue into a queue-of-output-queue.
        if need_coordinator:
            # to maximize concurrency
            self.queue_of_output_q = _make_q(n=1024 * 1024)

            # Protect input.get() and ouput.put(), only used by non-dispatcher
            # mode
            self.keep_order_lock = threading.RLock()

            self.coordinator_thread = start_thread(self._coordinate)
        else:
            self.queue_of_output_q = None

        # `dispatcher` is a user-defined function to distribute args to workers.
        # It accepts the same args passed to worker and returns a number to
        # indicate which worker to used.
        if self.dispatcher is not None:
            self.dispatch_queues = []
            for i in range(self.n_thread):
                self.dispatch_queues.append({
                    'input': _make_q(),
                    'output': _make_q(),
                })
            self.dispatcher_thread = start_thread(self._dispatch)

        self.add_worker_thread()

    def add_worker_thread(self):

        with self.worker_group_lock:

            if self.exiting:
                logger.info('worker_group exiting.'
                            ' Thread number change not allowed')
                return

            s, e = self.running_index_range

            for i in range(s, e):

                if i not in self.threads:

                    if self.keep_order:
                        th = start_thread(self._exec_in_order,
                                          self.input_queue, _make_q(), i)
                    elif self.dispatcher is not None:
                        # for worker group with dispatcher, threads are added for the first time
                        assert s == 0
                        assert e == self.n_thread
                        th = start_thread(self._exec,
                                          self.dispatch_queues[i]['input'],
                                          self.dispatch_queues[i]['output'],
                                          i)
                    else:
                        th = start_thread(self._exec,
                                          self.input_queue, self.output_queue, i)

                    self.threads[i] = th

    def _exec(self, input_q, output_q, thread_index):

        while self.running:

            # If this thread is not in the running thread range, exit.
            if thread_index < self.running_index_range[0]:

                with self.worker_group_lock:
                    del self.threads[thread_index]

                logger.info('worker-thread {i} quit'.format(i=thread_index))
                return

            args = input_q.get()
            if args is Finish:
                return

            with self.probe['probe_lock']:
                self.probe['in'] += 1

            try:
                rst = self.worker(args)
            except Exception as e:
                logger.exception(repr(e))
                continue

            finally:
                with self.probe['probe_lock']:
                    self.probe['out'] += 1

            # If rst is an iterator, it procures more than one args to next job.
            # In order to be accurate, we only count an iterator as one.

            _put_rst(output_q, rst)

    def _exec_in_order(self, input_q, output_q, thread_index):

        while self.running:

            if thread_index < self.running_index_range[0]:

                with self.worker_group_lock:
                    del self.threads[thread_index]

                logger.info('in-order worker-thread {i} quit'.format(
                    i=thread_index))
                return

            with self.keep_order_lock:

                args = input_q.get()
                if args is Finish:
                    return
                self.queue_of_output_q.put(output_q)

            with self.probe['probe_lock']:
                self.probe['in'] += 1

            try:
                rst = self.worker(args)

            except Exception as e:
                logger.exception(repr(e))
                output_q.put(EmptyRst)
                continue

            finally:
                with self.probe['probe_lock']:
                    self.probe['out'] += 1

            output_q.put(rst)

    def _coordinate(self):

        while self.running:

            outq = self.queue_of_output_q.get()
            if outq is Finish:
                return

            _put_rst(self.output_queue, outq.get())

    def _dispatch(self):

        while self.running:

            args = self.input_queue.get()
            if args is Finish:
                return

            n = self.dispatcher(args)
            n = n % self.n_thread

            queues = self.dispatch_queues[n]
            inq, outq = queues['input'], queues['output']

            self.queue_of_output_q.put(outq)
            inq.put(args)


class JobManager(object):

    def __init__(self, workers, queue_size=1024, probe=None, keep_order=False):

        if probe is None:
            probe = {}

        self.workers = workers
        self.head_queue = _make_q(queue_size)
        self.probe = probe
        self.keep_order = keep_order

        self.worker_groups = []

        self.probe.update({
            'worker_groups': self.worker_groups,
            'probe_lock': threading.RLock(),
            'in': 0,
            'out': 0,
        })

        self.make_worker_groups()

    def make_worker_groups(self):

        inq = self.head_queue

        workers = self.workers + [_blackhole]
        for i, worker in enumerate(workers):

            if callable(worker):
                worker = (worker, 1)

            # worker callable, n_thread, dispatcher
            worker = (worker + (None,))[:3]

            worker, n, dispatcher = worker

            wg = WorkerGroup(i, worker, n, inq,
                             dispatcher,
                             self.probe, self.keep_order)

            self.worker_groups.append(wg)
            inq = wg.output_queue

    def set_thread_num(self, worker, n):

        # When thread number is increased, new threads are created.
        # If thread number is reduced, we do not stop worker thread in this
        # function.
        # Because we do not have a steady way to shutdown a running thread in
        # python.
        # Worker thread checks if it should continue running in _exec() and
        # _exec_in_order(), by checking its thread_index against running thread
        # index range running_index_range.

        assert(n > 0)
        assert(isinstance(n, int))

        for wg in self.worker_groups:

            """
            In python2, `x = X(); x.meth is x.meth` results in a `False`.
            Every time to retrieve a method, python creates a new **bound** function.

            We must use == to test function equality.

            See https://stackoverflow.com/questions/15977808/why-dont-methods-have-reference-equality
            """

            if wg.worker != worker:
                continue

            if wg.dispatcher is not None:
                raise JobWorkerError('worker-group with dispatcher does not allow to change thread number')

            with wg.worker_group_lock:

                if wg.exiting:
                    logger.info('worker group exiting.'
                                ' Thread number change not allowed')
                    break

                s, e = wg.running_index_range
                oldn = e - s

                if n < oldn:
                    s += oldn - n
                elif n > oldn:
                    e += n - oldn
                else:
                    break

                wg.running_index_range = [s, e]
                wg.add_worker_thread()

                logger.info('thread number is set to {n},'
                            ' thread index: {idx},'
                            ' running threads: {ths}'.format(
                                n=n,
                                idx=range(wg.running_index_range[0],
                                          wg.running_index_range[1]),
                                ths=sorted(wg.threads.keys())))
                break

        else:
            raise JobWorkerNotFound(worker)

    def put(self, elt):
        self.head_queue.put(elt)

    def join(self, timeout=None):

        endtime = time.time() + (timeout or 86400 * 365)

        for wg in self.worker_groups:

            with wg.worker_group_lock:
                # prevent adding or removing thread
                wg.exiting = True
                ths = wg.threads.values()

            if wg.dispatcher is None:
                # put nr = len(threads) Finish
                for th in ths:
                    wg.input_queue.put(Finish)
            else:
                wg.input_queue.put(Finish)
                # wait for dispatcher to finish or jobs might be lost
                wg.dispatcher_thread.join(endtime - time.time())

                for qs in wg.dispatch_queues:
                    qs['input'].put(Finish)

            for th in ths:
                th.join(endtime - time.time())

            if wg.queue_of_output_q is not None:
                wg.queue_of_output_q.put(Finish)
                wg.coordinator_thread.join(endtime - time.time())

            # if join timeout, let threads quit at next loop
            wg.running = False

    def stat(self):
        return stat(self.probe)


def run(input_it, workers, keep_order=False, timeout=None, probe=None):

    mgr = JobManager(workers, probe=probe, keep_order=keep_order)

    try:
        for args in input_it:
            mgr.put(args)

    finally:
        mgr.join(timeout=timeout)


def stat(probe):

    with probe['probe_lock']:
        rst = {
            'in': probe['in'],
            'out': probe['out'],
            'doing': probe['in'] - probe['out'],
            'workers': [],
        }

    # exclude the _start and _end
    for wg in probe['worker_groups'][:-1]:
        o = {}
        wk = wg.worker
        o['name'] = (wk.__module__ or 'builtin') + ":" + wk.__name__
        o['input'] = _q_stat(wg.input_queue)
        if wg.dispatcher is not None:
            o['dispatcher'] = [
                {'input': _q_stat(qs['input']),
                 'output': _q_stat(qs['output']),
                 }
                for qs in wg.dispatch_queues
            ]

        if wg.queue_of_output_q is not None:
            o['coordinator'] = _q_stat(wg.queue_of_output_q)

        s, e = wg.running_index_range
        o['nr_worker'] = e - s

        rst['workers'].append(o)

    return rst


def _q_stat(q):
    return {'size': q.qsize(),
            'capa': q.maxsize
            }


def _put_rst(output_q, rst):

    if isinstance(rst, types.GeneratorType):
        for rr in rst:
            _put_non_empty(output_q, rr)
    else:
        _put_non_empty(output_q, rst)


def _blackhole(args):
    return EmptyRst


def _put_non_empty(q, val):
    if val is not EmptyRst:
        q.put(val)


def _make_q(n=1024):
    return Queue.Queue(n)


def start_thread(exec_func, *args):
    kwargs = {}
    t = threading.Thread(target=exec_func, args=args, kwargs=kwargs)
    t.daemon = True
    t.start()

    return t


def load_conf(fpath):
    with open(fpath, 'rb') as f:
        con = f.read()
        adchar = chardet.detect(con)
        encoding = adchar.get('encoding')
        if encoding is not None:
            con = con.decode(encoding)

        conf = yaml.safe_load(con)

    rst = {}
    for k, v in conf.items():
        if not isinstance(v, basestring):
            rst[k] = v
            continue

        logger.info("load conf k: %s, v type: %s" % (k, type(v)))
        if not isinstance(v, unicode):
            adchar = chardet.detect(v)
            encoding = adchar.get('encoding')
            if encoding is not None:
                v = v.decode(encoding)

        rst[k] = v

    if len(ak) > 0:
        rst['accesskey'] = ak

    if len(sk) > 0:
        rst['secretkey'] = sk

    return rst


def get_files(conf):
    rst = []
    src_dir = conf['src_dir']
    ignore = conf['ignore']

    if not os.path.exists(src_dir):
        logger.error(u'{d} not exist'.format(d=src_dir))
        return

    if os.path.isfile(src_dir):
        rst.append(src_dir)
        return rst

    for root, _, files in os.walk(src_dir):
        for fn in files:
            fp = os.path.join(src_dir, root, fn)
            if ignore is not None and len(ignore) > 0 and re.match(ignore, fp) is not None:
                continue

            rst.append(fp)

    return rst


def get_key(conf, fpath):
    if conf['keep_path']:
        key = os.path.relpath(fpath, conf['src_dir'])
    else:
        key = os.path.basename(fpath)

    key = '/'.join(key.split('\\'))
    while key.find('//') != -1:
        key = key.replace('//', '/')

    if isinstance(key, str):
        key = key.decode('utf-8')

    return u"{pre}{key}".format(pre=conf['prefix'], key=key)


def get_client(conf):
    ak = conf['accesskey'].encode('utf-8')
    sk = conf['secretkey'].encode('utf-8')
    bk = conf['bucket'].encode('utf-8')
    h = s2apiclient.S3(ak, sk, bk)
    h.need_auth = True
    return h


def check_exist(cli, key):
    try:
        uri = cli._get_uri("HEAD", key)
        cli._http_requst("HEAD", uri, httpcode=200)
    except Exception:
        return False

    return True


def _md5(buf):
    return hashlib.md5(buf).hexdigest().lower()


def upload_part(cli, key, upid, partnum, buf):
    for _ in range(20):
        try:
            return cli.upload_part_data(key, upid, partnum, buf)
        except Exception as e:
            logger.exception(repr(e) + repr((key, partnum)))
            time.sleep(1)

    else:
        raise e


def upload(cli, fpath, bucket, key):
    fsize = os.path.getsize(fpath)
    if fsize < 128 * MB:
        return cli.upload_file(key, fpath)

    upid = cli.get_upload_id(key)
    partnum = 0
    mergedata = '<?xml version="1.0" encoding="UTF-8"?><CompleteMultipartUpload>'
    with open(fpath, 'rb') as f:
        while True:
            buf = f.read(1024*1024*32)
            if buf == '':
                break

            md5 = _md5(buf)
            partnum += 1
            upload_part(cli, key, upid, partnum, buf)
            mergedata += '<Part><PartNumber>%d</PartNumber><ETag>%s</ETag></Part>' % (partnum, md5.lower())

    mergedata += '</CompleteMultipartUpload>'

    for _ in range(20):
        try:
            cli.merge_parts_data(key, upid, mergedata)
            return
        except Exception as e:
            logger.exception(repr(e) + repr(key))
            time.sleep(1)
    else:
        raise e


def try_upload(args):
    conf, fpath, key = args
    cli = get_client(conf)

    for _ in range(3):
        try:
            if not conf['overwrite'] and check_exist(cli, key):
                logger.info(u"do not overwrite existed file {key}".format(key=key))
                return 0, fpath

            upload(cli, fpath, conf['bucket'], key)
            return 0, fpath

        except Exception as e:
            logger.exception(repr(e) + repr(fpath))
            continue

    return (None, fpath)


st = {'succ': 0, 'fail': 0, 'fail_files': []}
st_lock = threading.RLock()


def upload_res(args):
    s, fpath = args
    if s != 0:
        logger.error(u'failed to upload: {fp} st: {st}'.format(fp=fpath, st=s))
        with st_lock:
            st['fail'] += 1
            st['fail_files'].append(args[1])
    else:
        with st_lock:
            st['succ'] += 1


def progress(total):
    f = st['fail']
    s = st['succ']
    msg = 'failed count: {f:<10} success count: {s:<10} progress: {a:>10} / {t:<10} {p:.0%}'.format(
            f=f, s=s, a=f+s, t=total, p=float((f+s)) / total)

    print(msg)
    logger.info(u'upload state: {st}'.format(st=msg))


def print_progress(total):
    while True:
        progress(total)
        time.sleep(1)


if __name__ == '__main__':
    conf = load_conf('./upload_dir.yaml')
    files = get_files(conf)
    if files is None:
        time.sleep(1)
        sys.exit(1)

    def iter():
        for f in files:
            k = get_key(conf, f)
            yield (conf, f, k)

    thread_count = conf.get('thread_count', None)
    if thread_count is None:
        thread_count = 30

    start_thread(print_progress, len(files))
    run(iter(), [(try_upload, thread_count), (upload_res, 1)], timeout=86400*100)
    progress(len(files))

    if len(st['fail_files']) > 0:
        logger.error(u"failed to upload: {fs}".format(fs=st['fail_files']))

    time.sleep(10)
