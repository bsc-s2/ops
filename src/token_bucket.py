#!/usr/bin/env python2
# coding:utf-8

import logging
import random
import threading
import time
from datetime import datetime

MB = 1024 ** 2
random.seed(time.time())

logger = logging.getLogger(__name__)


class TokenBucket(object):

    def __init__(self, speed_conf):
        self.speed_in_hours = self.init_speeds(speed_conf)
        self.lock = threading.RLock()
        self.feed_second = int(time.time()) - 1
        self.token_left = 0

    def get_bucket_size(self):
        hour = datetime.now().hour
        bucket_size = int(self.speed_in_hours[hour] * MB)
        return bucket_size

    def _feed(self, curr_second):
        if self.feed_second == curr_second:
            return

        bucket_size = self.get_bucket_size()

        to_feed = (curr_second - self.feed_second) * bucket_size

        actual_feed = min(to_feed, bucket_size - self.token_left)

        self.token_left += actual_feed

        self.feed_second = curr_second

        logger.info('after feed at time: %d, now left: %d, fed: %d' %
                    (curr_second, self.token_left, actual_feed))

    def feed(self):
        curr_second = int(time.time())
        if self.feed_second == curr_second:
            return

        with self.lock:
            self._feed(curr_second)

    def try_get_tokens(self, n):
        self.feed()

        if self.token_left > 0:
            with self.lock:
                self.token_left -= n
            return n

        return None

    def get_tokens(self, n):
        while True:
            got = self.try_get_tokens(n)
            if got == n:
                logger.info('at time: %f, got %d tokens' % (time.time(), n))
                return n

            ts_now = time.time()
            to_sleep = 1 - (ts_now % 1) + random.random() % 0.1

            logger.info('at time: %f, no token left, to sleep: %f second' %
                        (ts_now, to_sleep))

            time.sleep(to_sleep)

    def init_speeds(self, speed_conf):
        speed_in_hours = [-1] * 24

        for hour, speed in speed_conf.iteritems():
            speed_in_hours[int(hour)] = speed

        for i in range(0, 48):
            cur = i % 24
            pre = (i - 1) % 24
            if speed_in_hours[cur] == -1:
                speed_in_hours[cur] = speed_in_hours[pre]

        return speed_in_hours
