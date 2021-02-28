
import logging
import unittest
import enum
from collections import namedtuple

logger = logging.getLogger('txn_manager')

class AccessType(enum.Enum):
    read = 0
    write = 1

Access = namedtuple('Access', ['type', 'variable', 'value', 'version'])

class Transaction(object):
    def __init__(self, tid, timestamp):
        self._tid = tid
        self._timestamp = timestamp
        self._accesses = []
        self._writes = []

        self._rlocks = set()
        self._wlocks = set()
        self._abort = False
        self._reason = None

        self._accessed_sites = set()

        #self.blocked_sites = None

    def __repr__(self):
        return 'T{}@{}'.format(self._tid, self._timestamp)

    def fail_site(self, site):
        if site in self._accessed_sites:
            self.abort_fail(site)

    @property
    def even_writes(self):
        ws = []
        for ac in self._accesses:
            if ac.type == AccessType.write and ac.variable % 2 == 0:
                ws.append(ac.variable)
        return ws

    @property
    def tid(self):
        return self._tid

    @property
    def timestamp(self):
        return self._timestamp

    @property
    def read_only(self):
        return False

    @property
    def all_locks(self):
        return self._rlocks.union(self._wlocks)

    @property
    def abort_reason(self):
        return self._reason

    def add_rlock(self, site, var):
        self._rlocks.add((site, var))

    def has_rlock(self, site, var):
        return (site, var) in self._rlocks or (site, var) in self._wlocks

    def add_wlock(self, site, var):
        self._wlocks.add((site, var))

    def has_wlock(self, site, var):
        return (site, var) in self._wlocks

    def abort_dl(self):
        self._abort = True
        self._reason = 'deadlock'

    def abort_fail(self, site):
        self._abort = True
        self._reason = 'site {} failure'.format(site)

    def write(self, var, value, sites):
        self._accesses.append(
            Access(AccessType.write, var, value, self.timestamp))
        self._accessed_sites = self._accessed_sites.union(sites)

        def flush(DB, ts, sites=sites, var=var, value=value):
            for s in sites:
                DB[s].write(var, value, ts)
        self._writes.append(flush)

    def read(self, var, mval, site):
        self._accesses.append(
            Access(AccessType.read, var, mval.value, mval.version))
        self._accessed_sites.add(site)

    def commit(self):
        """
            Checks that transaction can commit and drops all locks
        """
        return not self._abort, self._writes

class ReadOnlyTransaction(Transaction):
    @property
    def read_only(self):
        return True

    def write(self, *args):
        raise AttributeError('Cannot write in a RO txn')

    def __repr__(self):
        return super().__repr__() + '(RO)'

    def commit(self):
        return not (self._abort and not (self._abort)), []

def set_testvalsT():
    """
        For unit testing class Transaction - JL
    """
    obj = Transaction(3, 20)
    obj._accessed_sites = {(2, 5)}
    obj.fail_site(4)
    obj._rlocks = {(6, 5)}
    obj._wlocks = {(8, 7)}

    return obj

def set_testvalsROT():
    """
        For unit testing class ReadOnlyTransaction - JL
    """
    pass


# JL
class TestTransaction(unittest.TestCase):
    def setUp(self):
        self.t = set_testvalsT()

    def test__repr__(self):
        self.assertEqual(self.t.__repr__(), ('T3@20'))

    def test_fail_site(self):
        self.assertFalse(self.t._abort)

    def test_tid(self):
        self.assertEqual(self.t.tid, (3))

    def test_timestamp(self):
        self.assertEqual(self.t.timestamp, (20))

    def test_read_only(self):
        self.assertFalse(self.t.read_only)

    def test_all_locks(self):
        self.assertEqual(self.t.all_locks, {(6, 5), (8, 7)})

    def test_add_rlock(self):
        self.t.add_rlock(2, 1)
        self.assertEqual(self.t._rlocks, {(6, 5), (2, 1)})

    def test_has_rlock(self):
        self.t.add_rlock(2, 1)
        self.assertTrue(self.t.has_rlock(2, 1), {(6, 5), (2, 1)})

    def test_has_wlock(self):
        self.t.add_wlock(4, 14)
        self.assertTrue(self.t.has_wlock(4, 14), {(4, 14)})

    def test_abort(self):
        self.t.abort()
        self.assertTrue(self.t._abort)

    def test_write(self):
        pass

    def test_read(self):
        pass

    def test_commit(self):
        self.t.abort()
        self.assertEqual(self.t.commit(), (False, []))

class TestReadOnlyTransaction(unittest.TestCase):
    def setUp(self):
        self.r = ReadOnlyTransaction(4, 21)

    def test_read_only(self):
        self.assertTrue(self.r.read_only)

    def test_write(self):
        with self.assertRaises(AttributeError):
            self.r.write((10, 19))


if __name__ == '__main__':
    unittest.main()