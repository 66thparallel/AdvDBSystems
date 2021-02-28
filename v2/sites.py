"""
Authors Conrad Christensen and Jane Liu

Classes:
    Site: where data is actually held in the database. Also holds a lock
          manager for the items.
    SiteEntry: tracks versions of data items for multi version concurrency
               control.
    TestSite: Unit tests for Site and SiteEntry classes
"""
import logging
import unittest
from collections import namedtuple

from .lock_manager import LockManager

logger = logging.getLogger('txn_manager')

MValue = namedtuple('MValue', ['value', 'version'])

class SiteEntry(object):
    """
        SiteEntry: This class provides a means for tracking versions of DB
            values. It allows failing and refuses reads that are before the
            latest fail point.

            SiteEntry holds 10 or 12 variables all indexed from 1 to 20. Each
            of those variables may have many versions.
    """
    def __new__(cls, zindex, sindex):
        """
            Controls object creation. Checks if zindex (variable) should be
            stored at sindex (site) and create entry if site index is even,
            or if entry-index mod 10 = site index.
        """
        if (zindex+1) % 2 == 1 and (zindex+2) % 10 != sindex:
            return None
        else:
            return super().__new__(cls)

    def __init__(self, zindex, sindex):
        self._index = zindex + 1
        self._sindex = sindex
        self._values = [MValue(self._index*10, 0)]
        self._isfailed = False
        self._fail_version = -1

    @property
    def version(self):
        if self.failed:
            raise ValueError('Reading from failed site entry')
        return self._values[0].version

    @property
    def value(self):
        if self.failed:
            raise ValueError('Reading from failed site entry')
        return self._values[0].value

    @property
    def latest(self):
        return self._values[0] # Return both value and version

    @property
    def failed(self):
        return self._isfailed

    @property
    def failed_at(self, version):
        return self._fail_version >= version

    def read_atbefore(self, version):
        """
            Read the value of this SiteEntry for version at or equal the given
            version number. There will at least by one that will succeed as all
            timestamps will be greater than 0, the starting version.

            Args:
                version: Timestamp of the RO transaction that all read versions
                         should proceed.

            Returns:
                An MValue containing the value read and version

            Raises:
                ValueError: Raised if no valid version found, or due to past
                            failure
        """
        ret = None
        for item in self._values:
            if item.version <= version:
                ret = item
                break
        if ret is None or ret.version <= self._fail_version:
            raise ValueError('Reading bad value {}'.format(ret))
        return ret # Return both value and version


    def fail(self):
        if self._index % 2 == 0:
            self._fail_version = self.version
            self._isfailed = True
        # else this is the only copy of the data so failure is irrelevant 
        # as reading is immediately available once the site parent recovers

    # TODO: Does this imply committed
    def write(self, new_value, new_version):
        self._values.insert(0, MValue(new_value, new_version))
        self._isfailed = False


class Site(object):
    def __init__(self, site_number):
        # Simpler to do this here
        self._site_number = site_number + 1
        self._db = [SiteEntry(i, self._site_number) for i in range(20)]
        self._lm = LockManager(20)
        self._isfailed = False

    def __getitem__(self, index):
        if index - 1 < 0 or index > len(self._db):
            raise ValueError('Illegal index {}. Must be between 1 and 20'
                             .format(index))
        if self._isfailed:
            raise AttributeError('Cannot access from failed DB')

        return self._db[index - 1]

    def __setitem__(self, *args):
        raise AttributeError('Not allowed')

    def bypass_failed(self, index):
        """
            Sets is_failed to false before reading.
        """
        fail_state = self._isfailed
        self._isfailed = False
        ret = self[index]
        self._isfailed = fail_state
        return ret

    def unlock(self, tid):
        return self._lm.unlock(tid)

    def read(self, var, txn):
        """
            Checks if transaction is read only. Gets lock and performs read. If
            transaction is read only, then no lock is acquired and only versions
            less than or equal to the read-only version are read.
        """
        if txn.read_only:
            mval = self[var].read_atbefore(txn.timestamp)
        else:
            if not txn.has_rlock(self._site_number, var):
                locked = self._lm.rlock(var, txn.tid)
                if locked:
                    txn.add_rlock(self._site_number, var)
                    mval = self[var].latest 
                else:
                    mval = None
            else:
                mval = self[var].latest

        return mval

    def write_lock(self, var, txn):
        """
            Attempts to acquire write lock, and enqueues if fails
        """
        if not txn.has_wlock(self._site_number, var):
            if txn.has_rlock(self._site_number, var):
                locked = self._lm.upgrade(var, txn.tid)
            else:
                locked = self._lm.wlock(var, txn.tid)
                if locked:
                    txn.add_wlock(self._site_number, var)
        else:
            locked = True
        return locked

    def write(self, var, value, timestep):
        self[var].write(value, timestep)

    @property
    def failed(self):
        return self._isfailed

    def fail(self):
        # TODO: Consider a drop-all method for the _lm instead of just creating
        # a new lock manager
        self._lm = LockManager(20)
        self._isfailed = True
        for entry in self._db:
            if entry:
                entry.fail()

    def recover(self):
        self._isfailed = False

    def dl_detect(self, edges):
        return self._lm.dl_detect(edges)


class TestSite(unittest.TestCase):
    def setUp(self):
        self._site1 = Site(0)
        self._site2 = Site(1)

    def test_setup(self):
        self.assertTrue(self._site1 is not None)

    def test_site1(self):
        self.assertTrue(self._site1[1] is None)
        self.assertTrue(self._site1[11] is None)
        self.assertTrue(self._site2[1] is not None)
        self.assertTrue(self._site2[11] is not None)
        self.assertTrue(self._site2[2] is not None)
        self.assertTrue(self._site2[12] is not None)

        self.assertTrue(self._site2[13] is None)
        self.assertTrue(self._site2[15] is None)


if __name__ == '__main__':
    unittest.main()
