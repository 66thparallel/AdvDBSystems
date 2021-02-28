'''
Tests written that every line of code will be executed at least once
Test: ask for read lock twice from same transaction. should say yes twice.
Test: dl_detect() function
Test: t1 --> t2 --> t4 --> t3

document functions, what they do, and add loggers statements - add your initials to function
add unit tests - deadlock detection - add your initials

Questions
1. What does varc in LockManager() stand for?

dfs - directed graph with nodes of txns. find cycles
'''

import logging
import unittest

logger = logging.getLogger('txn_manager')

class Lock(object):
    read = 17
    write = 23

    def __init__(self):
        """
            _lh stands for 'lock holder' and _q is 'queue'. Both _lh and _q contain
            tuples of lock type and transaction id (tid). _lh contains transactions
            holding locks. q contains transactions waiting to get a lock.
        """
        self._lh = []
        self._q = []

    def add_edges(self, edges):
        """
            Read lock detection goes through all variables and adds edges. Returns
            set of all items in cycle. Adds all keys.
        """
        for _, v in self._lh + self._q:
            if edges[v] == []:
                pass

        # Anything in q is waiting for transactions before it in the queue and for
        # transactions holding locks.
        for i in reversed(range(len(self._q))):
            # Starts from last index (10 elements, starts at 9)
            for j in reversed(range(i)):
                if self._q[i][1] != self._q[j][1]:
                    edges[self._q[i][1]].add(self._q[j][1])
            for l in self._lh:
                if self._q[i][1] != l:
                    edges[self._q[i][1]].add(l[1])
        return edges

    # Returns the number of locks held.
    @property
    def held(self):
        return len(self._lh) > 0

    # Checks if q and lock holder are empty and if so, gives lock. Also checks
    # if a site is ready to read, meaning q is empty but another transaction is
    # holding the read lock, so a transaction can jump in and get a read lock.
    @property
    def read_ready(self):
        return self._q == [] and self.held and self._lh[0][0] == Lock.read

    # Determines if tid (transaction id) already has a lock or is waiting.
    # Returns true or false. Tries to acquire root lock.
    def rlock(self, tid):
        if (Lock.read, tid) in self._lh or (Lock.write, tid) in self._lh:
            return True
        elif (Lock.read, tid) in self._q:
            return False

        if self._q == [] and self._lh == []:
            self._lh.append((Lock.read, tid))
            return True
        elif self.read_ready:
            self._lh.append((Lock.read, tid))
            return True
        else: # Only thing in q could be write lock
            self._q.append((Lock.read, tid))
            return False

    # Does the same as rlock() except for writes.
    def wlock(self, tid):
        if (Lock.write, tid) in self._lh:
            return True
        elif (Lock.write, tid) in self._q:
            return False

        if self._q == [] and self._lh == []:
            self._lh.append((Lock.write, tid))
            return True
        else:
            self._q.append((Lock.write, tid))
            return False

    # upgrade() takes a transaction out of lock holders and puts it at front of
    # q so it can get a lock. Must wait for other read transactions to vacate the
    # queue in front of it. An example is if T1 wants to execute R1(x) and W1(x),
    # T2 wants to execute R2(x), and T3 wants to execute W3(x).
    def upgrade(self, tid):
        if (Lock.write, tid) in self._lh:
            return True

        self._lh.remove((Lock.read, tid))
        if not self.held:
            self._lh.append((Lock.write, tid))
            return True
        else:
            self._q.insert(0, (Lock.write, tid))
            return False

    # unlock() notifies whether a transaction can lock a site. It can also unlock a site
    # and remove the tid (transaction id) from lock holders.
    def unlock(self, tid):
        if (Lock.read, tid) in self._lh:
            self._lh.remove((Lock.read, tid))
        if (Lock.write, tid) in self._lh:
            self._lh.remove((Lock.write, tid))

        if (Lock.read, tid) in self._q:
            self._q.remove((Lock.read, tid))
        if (Lock.write, tid) in self._q:
            self._q.remove((Lock.write, tid))

        to_notify = []
        if (not self.held) and len(self._q) > 0:
            first = self._q.pop(0)
            self._lh.append(first)
            to_notify.append(first[1])
            if first[0] == Lock.read:
                while len(self._q) > 0 and self._q[0][0] == Lock.read:
                    next_ = self._q.pop(0)
                    self._lh.append(next_)
                    to_notify.append(next_[1])
        return to_notify


class LockManager(object):
    def __init__(self, varc):
        self._lock_q = [Lock() for _ in range(varc)]

    def rlock(self, var, tid):
        vindex = var - 1
        return self._lock_q[vindex].rlock(tid)

    def wlock(self, var, tid):
        vindex = var - 1
        return self._lock_q[vindex].wlock(tid)

    def upgrade(self, var, tid):
        vindex = var - 1
        return self._lock_q[vindex].upgrade(tid)

    def unlock(self, tid):
        updates = []
        for lock in self._lock_q:
            updates += lock.unlock(tid)
        return updates

    def leave_q(self, var, tid):
        self._lock_q[var - 1]._q.remove((Lock.write, tid))

    def dl_detect(self, edges):
        for lock in self._lock_q:
            lock.add_edges(edges)


class TestLockManager(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from collections import defaultdict
        cls.dd = defaultdict

    def setUp(self):
        self._lm = LockManager(20)

    def test_notify(self):
        self.assertTrue(self._lm.rlock(1, 1))
        self.assertTrue(self._lm.rlock(1, 2))
        self.assertFalse(self._lm.wlock(1, 3))
        self.assertEqual(self._lm.unlock(1), [])
        self.assertEqual(len(self._lm.unlock(2)), 1)
        self.assertTrue(self._lm._lock_q[0].held)

    # TODO: Change assertTrue(a == b) into assertEqual(a, b)
    def test_upgrade(self):
        self.assertTrue(self._lm.rlock(1, 1))
        self.assertTrue(self._lm.rlock(1, 2))

        self.assertFalse(self._lm.wlock(1, 3))
        self.assertFalse(self._lm.upgrade(1, 2))

        self.assertTrue(self._lm.unlock(1) == [2])
        self.assertTrue(self._lm._lock_q[0]._lh[0] == (Lock.write, 2))
        self.assertTrue(self._lm.unlock(2) == [3])
        self.assertTrue(self._lm._lock_q[0]._lh[0] == (Lock.write, 3))

    # TODO: Change assertTrue(a == b) into assertEqual(a, b)
    def test_read_q(self):
        self.assertTrue(self._lm.rlock(1, 1))
        self.assertTrue(self._lm.rlock(1, 2))

        self.assertFalse(self._lm.wlock(1, 3))
        self.assertFalse(self._lm.rlock(1, 4))
        self.assertFalse(self._lm.rlock(1, 5))

        self.assertTrue(self._lm.rlock(2, 5))

        self.assertTrue(self._lm.unlock(1) == [])
        self.assertTrue(self._lm.unlock(2) == [3])
        self.assertTrue(self._lm.unlock(3) == [4, 5])

    # TODO: Expand this test to check for the exact edges that should be added.
    def test_dl_detect(self):  # In progress
        dd = self.dd(lambda: set())

        self.assertTrue(self._lm.rlock(1, 1))
        self.assertTrue(self._lm.rlock(1, 2))
        self.assertFalse(self._lm.wlock(1, 3))
        # Send empty edges object and ensure that something was added to edges
        self._lm.dl_detect(dd)
        self.assertTrue(len(dd) > 0)

if __name__ == '__main__':
    unittest.main()
