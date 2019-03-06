#!/usr/bin/env python
# coding: utf-8

import copy
import logging
import threading
import time

from kazoo import exceptions
from kazoo.client import KazooState
from kazoo.exceptions import KazooException
from kazoo.exceptions import NoNodeError

from pykit import zkutil
from pykit import utfjson

from .exceptions import ConnectionLoss
from .exceptions import Deadlock
from .exceptions import NotLocked
from .exceptions import RetriableError
from .exceptions import TXTimeout
from .exceptions import UnlockNotAllowed
from .exceptions import UserAborted
from .exceptions import CommitError
from .status import COMMITTED
from .status import PAUSED
from .status import PURGED
from .status import UNKNOWN
from .zkstorage import ZKStorage

logger = logging.getLogger(__name__)

DEFAULT_TIMEOUT = 1 * 365 * 24 * 3600


class TXRecord(object):

    def __init__(self, k, v, version, values):
        self.k = k
        self.v = v             # the latest updated value
        self.version = version  # stat.version of zk node `k`
        self.values = values

    def __str__(self):
        return '{k}={v}; ver={ver}'.format(k=self.k, v=self.v, ver=self.version)


class ZKTransaction(object):

    def __init__(self, zk, txid=None, timeout=DEFAULT_TIMEOUT, lock_timeout=None):

        # Save the original arg for self.run()
        self._zk = zk

        self.zke, self.owning_zk = zkutil.kazoo_client_ext(zk)
        self.timeout = timeout
        self.lock_timeout = lock_timeout

        self.txid = txid
        self.expire_at = None
        self.tx_alive_lock = None

        # User locked and required keys
        self.got_keys = {}
        # Keys user need to commit
        self.modifications = {}

        self.state_lock = threading.RLock()
        self.connected = True

        self.tx_status = None

        self.zkstorage = ZKStorage(self.zke)
        self.zkstorage.max_value_history = 2

        self.zke.add_listener(self._on_conn_change)

    def _on_conn_change(self, state):

        logger.debug('state changed: {state}'.format(state=state,))

        with self.state_lock:
            if state == KazooState.LOST or state == KazooState.SUSPENDED:
                self.connected = False

    def lock_get(self, key, blocking=True, latest=True, timeout=None):

        # We use persistent lock(ephemeral=False)
        # thus we do not need to care about connection loss during locking
        # phase.
        # But we still need to check connection when creating journal.

        if key in self.got_keys:
            if latest and key in self.modifications:
                return copy.deepcopy(self.modifications[key])

            return copy.deepcopy(self.got_keys[key])

        if blocking:
            self.lock_key(key, timeout)
        else:
            locked, other_txid, ver = self.try_lock_key(key)
            if not locked:
                return None

        val, version = self.zkstorage.record.get(key)
        lvalue = val[-1]

        curr = TXRecord(k=key, v=copy.deepcopy(lvalue), version=version, values=val)
        self.got_keys[key] = curr

        rec = TXRecord(k=key, v=copy.deepcopy(lvalue), version=version, values=val)
        return rec

    def unlock(self, rec):

        if rec.k not in self.got_keys:
            raise NotLocked("Not allowed to unlock non-locked: {k}".format(k=rec.k))

        if rec.k in self.modifications:
            raise UnlockNotAllowed('{k} has set as changed'.format(k=rec.k))

        self.zkstorage.try_release_key(self.txid, rec.k)

        del self.got_keys[rec.k]

    def set(self, rec):
        logger.info('{tx} tx.set: {rec}'.format(tx=self, rec=rec))
        self.modifications[rec.k] = rec

    def set_state(self, state_data):
        txst = {
            'got_keys': self.got_keys.keys(),
            'data': state_data,
        }

        self.zkstorage.state.set_or_create(self.txid, txst)

    def get_state(self, txid=None):

        txst, ver = self._get_state(txid=txid)
        if txst is None:
            return None
        else:
            return txst['data']

    def _get_state(self, txid=None):
        if txid is None:
            txid = self.txid
        txst, ver = self.zkstorage.state.get(txid)

        if txst is None:
            return None, -1

        return txst, ver

    def has_state(self, txid):
        st, _ = self.zkstorage.state.get(txid)
        return st is not None

    def delete_state(self, txid):
        try:
            self.zkstorage.state.delete(txid)
        except NoNodeError:
            pass

    def lock_key(self, key, timeout):

        try:
            self._lock_key(key, timeout)
        except zkutil.LockTimeout:
            raise TXTimeout('{tx} timeout waiting for lock: {key}'.format(tx=self, key=key))

        logger.info('{tx} [{key}] locked'.format(tx=self, key=key))

    def try_lock_key(self, key):

        while True:
            other_txid, ver = None, None
            for other_txid, ver in self.zkstorage.acquire_key_loop(
                    self.txid, key, timeout=-1):
                # Run only once if not locked.
                # Does no reach here if locked.
                break

            if other_txid is None:
                return True, self.txid, -1

            if self.is_tx_alive(other_txid) or self.has_state(other_txid):
                return False, other_txid, ver

            self.zkstorage.try_release_key(other_txid, key)

    def _lock_key(self, key, timeout):

        if timeout is None:
            timeout = self.lock_timeout

        if timeout is None:
            timeout = self.time_left()

        expire_at = time.time() + timeout

        def _time_left():
            return expire_at - time.time()

        for other_txid, ver in self.zkstorage.acquire_key_loop(
                self.txid, key, timeout=_time_left()):

            logger.info('{tx} wait[{key}]-> {other_txid} ver: {ver}'.format(
                tx=self, key=key, other_txid=txidstr(other_txid), ver=ver))

            # A tx that has state saved is recoverable, is treated as alive tx.
            if not self.is_tx_alive(other_txid) and not self.has_state(other_txid):
                # dead tx dont't save the journal because of unlock the key and save journal
                # are in a kazoo transaction. we can no nothing without the journal,
                # so we just unlock the key.
                self.zkstorage.try_release_key(other_txid, key)
                continue

            logger.info('{tx}: other tx {other_txid} is alive'.format(
                tx=self, other_txid=txidstr(other_txid)))

            if self.txid > other_txid:

                if len(self.got_keys) == 0:

                    # no locking, no deadlock
                    logger.info('{tx} wait[{key}]-> {other_txid} no-lock'.format(
                        tx=self, key=key, other_txid=txidstr(other_txid)))

                    for i in range(other_txid, self.txid):
                        self.wait_tx_to_finish(i, _time_left())
                    continue

                else:

                    # let earlier tx to finish first
                    logger.info('{tx} wait[{key}]-> {other_txid} deadlock'.format(
                        tx=self, key=key, other_txid=txidstr(other_txid)))

                    self.release_all_key_locks()
                    self.delete_state(self.txid)

                    for i in range(other_txid, self.txid):
                        self.wait_tx_to_finish(i, _time_left())

                    raise Deadlock('my txid: {mytxid} lockholder txid: {other_txid}'.format(
                        mytxid=self.txid, other_txid=other_txid))

            # self.txid < other_txid:
            # I am an earlier tx. I should run first.
            #
            # If there is no deadlock:
            #   the older tx will finish later. Then I wait.
            #
            # If there is a deadlock:
            #   the older tx will abort.
            logger.info('{tx} wait[{key}]-> {other_txid}'.format(
                tx=self, key=key, other_txid=txidstr(other_txid)))

    def wait_tx_to_finish(self, txid, timeout):

        logger.info('{tx} wait-> {other_txid}'.format(tx=self, other_txid=txidstr(txid)))

        try:
            zkutil.wait_absent(self.zke,
                               self.zke._zkconf.tx_alive(txid),
                               timeout=timeout)
        except zkutil.ZKWaitTimeout as e:

            logger.info(repr(e) + ' while waiting for other tx: {txid}'.format(
                txid=txid))

            raise TXTimeout('{tx} timeout waiting for tx:{txid}'.format(tx=self, txid=txid))

    def is_tx_alive(self, txid):
        try:
            self.zke.get(self.zke._zkconf.tx_alive(txid))
            logger.info('{tx} tx:{txid} is alive'.format(tx=self, txid=txid))
            return True
        except NoNodeError:
            logger.info('{tx} tx:{txid} is dead'.format(tx=self, txid=txid))
            return False

    def begin(self, txid=None):

        assert self.expire_at is None

        self.expire_at = time.time() + self.timeout

        if self.txid is None:

            if txid is not None:
                # Run a specified tx, normally when to recover a dead tx process
                self.txid = txid

            else:
                # Run a new tx, create txid.
                zstat = self.zke.set(self.zke._zkconf.txid_maker(), 'x')
                self.txid = zstat.version

        zkconf = copy.deepcopy(self.zke._zkconf.conf)
        zkconf['lock_dir'] = self.zke._zkconf.tx_alive()

        self.tx_alive_lock = zkutil.ZKLock(self.txid,
                                           zkconf=zkconf,
                                           zkclient=self.zke,
                                           timeout=self.expire_at-time.time())

        try:
            self.tx_alive_lock.acquire()
        except zkutil.LockTimeout as e:
            raise TXTimeout(repr(e) + ' while waiting for tx alive lock')

    def commit(self, force=False):

        # Only when commit, it is necessary to ensure connection still active:
        # Thus tx_alive_lock is not lost, then no other process would take
        # charge of this tx.

        if self.time_left() < 0:
            raise TXTimeout('{tx} timeout when committing'.format(tx=self))

        self._assert_connected()

        kazootx = self.zke.transaction()

        jour = {}
        cnf = self.zke._zkconf

        for k, rec in self.modifications.items():
            curr = self.got_keys[k]
            if rec.v == curr.v:
                continue

            jour[k] = rec.v
            if rec.v is None:
                if curr.version != -1:
                    kazootx.delete(cnf.record(rec.k), version=curr.version)
                continue

            record_vals = curr.values + [rec.v]
            record_vals = record_vals[-self.zkstorage.max_value_history:]

            if curr.version == -1:
                kazootx.create(cnf.record(rec.k),
                               utfjson.dump(record_vals),
                               acl=cnf.kazoo_digest_acl())
            else:
                kazootx.set_data(cnf.record(rec.k),
                                 utfjson.dump(record_vals),
                                 version=curr.version)

        state, ver = self._get_state(self.txid)
        if ver > -1:
            kazootx.delete(cnf.tx_state(self.txid), version=ver)

        for key in self.got_keys:
            kazootx.delete(cnf.lock(key), version=0)

        if len(jour) > 0 or force:
            # provide a str "journal_id", because
            # kazootx.create('xx/', '', sequence=True) => xx0000000000
            # kazootx.create('xx/a', '', sequence=True) => xx/a0000000000
            kazootx.create(cnf.journal("journal_id"),
                           utfjson.dump(jour),
                           acl=cnf.kazoo_digest_acl(),
                           sequence=True)
            rst = kazootx.commit()
            for r in rst:
                if isinstance(r, KazooException):
                    raise CommitError(rst)

            status = COMMITTED
            journal_id = rst[-1].split('/')[-1][-10:]
            self.zkstorage.add_to_journal_id_set(status, journal_id)
        else:
            # Nothing to commit, make it an aborted tx.
            kazootx.commit()
            status = PURGED

        self.tx_status = status
        self.modifications = {}
        self._close()

    def abort(self):
        raise UserAborted()

    def is_timeout(self):
        return self.time_left() > 0

    def time_left(self):
        return self.expire_at - time.time()

    def _close(self):

        self.release_all_key_locks()

        if self.tx_alive_lock is not None:

            try:
                self.tx_alive_lock.release()

            except (exceptions.ConnectionClosedError,
                    exceptions.ConnectionLoss) as e:

                logger.warn('{tx} {e} while release tx alive lock'.format(
                    tx=self, e=repr(e)))

            self.tx_alive_lock = None

        self.zke.remove_listener(self._on_conn_change)

        self.modifications = {}

        if self.owning_zk:
            zkutil.close_zk(self.zke)

    def release_all_key_locks(self):

        for key in self.got_keys:

            logger.info('{tx} releasing:[{key}]'.format(tx=self, key=key))

            try:
                self.zkstorage.try_release_key(self.txid, key)
            except (exceptions.ConnectionClosedError,
                    exceptions.ConnectionLoss) as e:

                logger.warn('{tx} {e} while release key lock {key}'.format(
                    tx=self, e=repr(e), key=key))
                break

        self.got_keys = {}

    def _assert_connected(self):
        with self.state_lock:
            if not self.connected:
                raise ConnectionLoss()

    def __enter__(self):
        self.begin()
        return self

    def __exit__(self, errtype, errval, _traceback):

        logger.info('{tx} __exit__ errtype:{errtype} errval:{errval}'.format(
            tx=self, errtype=errtype, errval=errval))

        try:
            if self.tx_status is None:

                try:
                    has = self.has_state(self.txid)
                    if has:
                        self.tx_status = PAUSED
                        self.got_keys = {}

                    else:
                        self.tx_status = PURGED
                except (exceptions.ConnectionClosedError,
                        exceptions.ConnectionLoss) as e:

                    logger.warn('{tx} {e} while get tx state'.format(
                                tx=self, e=repr(e)))

                if self.tx_status is None:
                    self.tx_status = UNKNOWN

            if errval is None:
                return True

            if isinstance(errval, UserAborted):
                if self.txid is not None:
                    self.delete_state(self.txid)

            return False

        finally:
            self._close()

    def __str__(self):
        return '{tx}[{locked}]'.format(tx=txidstr(self.txid),
                                       locked=','.join(sorted(self.got_keys.keys())))


def run_tx(zk, func, txid=None, timeout=None, lock_timeout=None, args=(), kwargs=None):

    if timeout is None:
        timeout = DEFAULT_TIMEOUT

    if kwargs is None:
        kwargs = {}

    expire_at = time.time() + timeout

    while True:

        tx = ZKTransaction(zk, timeout=expire_at - time.time(), lock_timeout=lock_timeout, txid=txid)
        txid = None

        try:
            with tx:
                func(tx, *args, **kwargs)

            assert tx.tx_status is not None, 'with-statement guarantees tx_status is not None'
            return tx.tx_status, tx.txid

        except RetriableError as e:
            logger.info('{tx} {e}'.format(tx=tx, e=repr(e)))
            continue


def list_recoverable(zk):

    t = ZKTransaction(zk)

    try:
        path = t.zkstorage.state.get_path('')
        children = t.zke.get_children(path)

        for txid in children:
            txid = int(txid)

            st = t.get_state(txid)
            if st is not None and not t.is_tx_alive(txid):
                yield txid, st

    finally:
        t._close()


def txidstr(txid):
    return 'tx-{txid:0>10}'.format(txid=txid)
