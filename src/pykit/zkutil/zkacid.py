import logging

from kazoo.exceptions import BadVersionError

from pykit import txutil

from .zkconf import kazoo_client_ext
from . import zkutil

logger = logging.getLogger(__name__)


def cas_loop(zkclient, path, json=True):

    zkclient, owning_zk = kazoo_client_ext(zkclient, json=json)

    def setter(path, val, zstat):
        zkclient.set(path, val, version=zstat.version)

    try:
        for curr in txutil.cas_loop(zkclient.get, setter, args=(path, ),
                                    conflicterror=BadVersionError):
            yield curr
    finally:
        if owning_zk:
            zkutil.close_zk(zkclient)
