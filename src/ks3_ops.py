import os
import logging

from ks3.connection import Connection
from ks3.prefix import Prefix
from ks3.key import Key

from pykit import shell
from pykit import logutil

logger = logging.getLogger(__name__)

fpath = './ks3_list_urls'
log_fpath = './ks3_ops.log'

def add_logger():
    log_file = os.path.join('./', log_fpath)

    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    file_handler = logging.FileHandler(log_file)
    formatter = logging.Formatter('[%(asctime)s, %(levelname)s] %(message)s')

    file_handler.setFormatter(formatter)

    logger.addHandler(file_handler)

    return logger

def make_cli(ak, sk, endpoint):
    return Connection(ak, sk, host=endpoint, is_secure=False, domain_mode=False)


def make_urls_to_file(ak, sk, bucket_name, endpoint):
    cli = make_cli(ak, sk, endpoint)
    blk = cli.get_bucket(bucket_name)

    with open(fpath, 'w') as fp:
        bundles = []
        n = 0

        for k in blk.list():
            logger.info("get key:" + k.name)

            if isinstance(k, Key):
                url = k.generate_url(3600 * 24 * 30 * 3)
                bundles.append(url + '\n')

            n = n + 1
            if n % 10 == 0:
                fp.writelines(bundles)
                fp.flush()
                os.fsync(fp.fileno())

                bundles = []
                n = 0

        if n > 0:
            fp.writelines(bundles)
            fp.flush()
            os.fsync(fp.fileno())

            bundles = []
            n = 0

    logger.info("finish list file urls to file")

if __name__ == "__main__":
    logger = add_logger()

    cmd = {
        'make_urls': (make_urls_to_file,
            ('ak',          {'type': str, 'help': 'specified your ks3 access key'}),
            ('sk',          {'type': str, 'help': 'specified your ks3 secert key'}),
            ('bucket_name', {'type': str, 'help': 'specified your ks3 bucket name'}),
            ('endpoint',    {'type': str, 'help': 'specified your ks3 endpoint'}),
        ),

        '__add_help__': {
            ('make_urls',): 'make ks3 bucket urls to ' + fpath + ' file',
        },

        '__description__': '''
        python2 ks3_ops.py make_urls <ak> <sk> <bucket_name> <endpoint>
        '''
    }

    shell.command(**cmd)
