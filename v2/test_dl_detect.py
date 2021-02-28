"""
Test for different cases of deadlock detection.

1. A long cycle deadlock, and a case where adding 1 txn creates 2 deadlocks,
but where it is the youngest txn so removing it fixes both deadlocks.

2. A single access from a txn causes 2 deadlocks, but this time each cycle
has its own youngest txn so that 2 txns must be aborted (i.e. a single write
access from some txn causes 2 other txn a to be aborted (each from some cycle
completed by the added edges from the write access) before the next command
is even processed)

"""

import unittest
import logging
from collections import defaultdict, namedtuple

MValue = namedtuple('MValue', ['value', 'version'])


class Lock(object):
    read = 17
    write = 23

    def __init__(self): # _lh stands for 'lock holder' and _q is 'queue'.
        self._lh = []   # lh holds tuples (lock_type, tid) of txns holding locks
        self._q = []    # q also holds tuples (lock_type, tid) of tnxs waiting for lock

class LockManager(object):
    def __init__(self):
        self._lock_q = [Lock() for _ in range(20)]

class SiteEntry(object):
    def __new__(cls, zindex, sindex):
        if (zindex+1) % 2 == 1 and (zindex+2) % 10 != sindex:
            return None
        else:
            # this returns a new SiteEntry(zindex, sindex) object
            return super().__new__(cls)

    def __init__(self, zindex, sindex):
        self._index = zindex + 1
        self._sindex = sindex
        self._values = [MValue(self._index*10, 0)]
        self._isfailed = False
        self._fail_version = -1
        # logger.info(self._values)

class Site(object):
    def __init__(self, site_number):
        # Simpler to do this here
        self._site_number = site_number + 1
        self._db = [SiteEntry(i, self._site_number) for i in range(20)]
        self._lm = LockManager()
        self._isfailed = False

class Database(object):
    def __init__(self):
        self._sites = []  # self._sites = Database() looks like [Site(0), Site(1),..., Site(9)]
        self._allsites = 10
        for i in range(self._allsites):
            self._sites.append(Site(i))

class Test(object):
    def __init__(self):
        self._sites = Database()

    def youngest(self, path):
        youngest = None
        for item in path:
            txn = self._cur_txns[item]
            if youngest is None or youngest[1] < txn.timestamp:
                youngest = (txn.tid, txn.timestamp)
        logger.info('DL detect abort triggered for txn {} on path {}'.format(youngest[0], path))
        return youngest[0]

    def tick(self):
        self._time += 1
        self.dl_detect()

    def dfs(v, edges, path):
        path.add(v)
        for n in edges[v]:
            if n in path or dfs(n, edges, path):
                return True
        path.remove(v)
        return False

    # Need these functions: self._sites._sites = Database()._sites, youngest()
    def dl_detect(self):
        """
            dl_detect works by collecting edges from each site then doing
            dfs on the edges
        """
        edges = defaultdict(lambda: set())
        for site in self._sites._sites:
            site.dl_detect(edges)
        edges = dict(edges)

        p_dead = None
        for v in edges:
            path = set()
            if dfs(v, edges, path):
                p_dead = path
                break

        if p_dead is not None:
            logger.info('DL detect at site {}'.format(site._site_number))
            self.abort(self.youngest(path))
            return

t = Test()
t._sites._sites.append()

