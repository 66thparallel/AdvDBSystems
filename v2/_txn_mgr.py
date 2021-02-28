'''
Questions:
1. What does __setitem__() in class Database do? Does it just notify the user that values can't be set in
2. Does TransactionManager.time() return the start time of all the transactions?
3. Is TransactionManager created once for every test case? Every time test case is run, the program is called.
4. What does the self.tick() do in new_txn()? This increases the time counter.

For mval in TransactionManager.read():
# There are 4 scenarios: blocked because of sites, read only transaction
# (blocked because of sites, not because of locks), perform the read and
# fails to get lock, or receives the lock and transaction successfully read and returned.


Write a lot of unit tests written by me (20 - 30) for all files. Multiple test case classes. Test Lock and LockManager.
Test every function, every branch. Call fcns enough times so every branch is hit at least once.

'''
import unittest
import logging
from collections import defaultdict

from .sites import Site
from .transaction import Transaction, ReadOnlyTransaction


logger = logging.getLogger('txn_manager')

class Database(object):
    """
        Creates a database of 10 Site objects. - JL
    """
    def __init__(self):
        self._sites = []
        self._allsites = 10
        for i in range(self._allsites):
            self._sites.append(Site(i))

    def __setitem__(self, *args):
        if args:
            raise ValueError("Now allowed. Cannot alter number of sites.")

    def __getitem__(self, index):
        idx = index - 1
        if idx < 0 or idx >= self._allsites:
            raise ValueError('{} does not exist. Value must be between 1 and 10'.format(idx))
        return self._sites[idx]

    def find_available(self, var, all=None): # change param to all=True when calling function find_all_available()
        if all:
            available = []
            for i in range(1, 11):
                site = self[i]
                if not site.failed and site[var]: available.append(i)
            return available
        else:
            for i in range(1, 11):
                site = self[i]
                if not site.failed and site[var] and (not site[var].failed): return i
            return None

class TransactionManager(object):
    """
        Manages transactions for each test case.
    """
    _test15_optimization = True

    def __init__(self, full_output=True, log_writes=True):
        self._full_output = full_output
        # Only log writes with full output
        self._log_writes = self._full_output and log_writes
        self._sites = Database()
        self._cur_txns = {}
        self._time = 1

        # This holds accesses that were blocked due to failed sites. This needs
        # to be checked when a site recovers, or there is a write to an even
        # numbered variable
        self._blocked_failed = set()

        self._blocked_2pl = set()

    @property
    def time(self):
        """
            Read only transactions use multiversion read consistency and only
            reads items committed before it starts. time() is a counter for
            whenever an event happens (read, write, etc.)
        """
        return self._time

    def new_txn(self, tid, read_only=False):
        self._cur_txns[tid] = (ReadOnlyTransaction if read_only
                               else Transaction)(tid, self.time)
        logger.info('New transaction {}'.format(self._cur_txns[tid]))
        self.tick()

    def finish_txn(self, tid):
        """
            Test cases don't always call end() for all transactions even when
            the transaction aborts. However, a deadlocked transaction needs to
            be aborted and deleted. When the test case file later calls end()
            it causes another deletion in the program leading to errors.
            This function handles those errors.
        """
        try:
            txn = self._cur_txns[tid]
        except KeyError as ke:
            return
        commit, writes = txn.commit()

        ws = None
        if commit:
            for wf in writes:
                wf(self._sites, self._time)
            logger.info('Commit transaction {}. Accesses: {}'
                        .format(self._cur_txns[tid], self._cur_txns[tid]._accesses))
            print('T{} commits'.format(tid))
            ws = txn.even_writes
        else:
            logger.info('Abort transaction {}. Accesses: {}'
                        .format(self._cur_txns[tid], self._cur_txns[tid]._accesses))
            reason = (' ({})'.format(txn.abort_reason) \
                      if self._full_output else '')
            print('T{} aborts{}'.format(tid, reason))

        # After finishing a transaction this releases all its locks. It also
        # notifies any other transactions waiting for a lock. Collects info about
        # who acquired a lock and runs unblock_2pl(). It checks to see who is
        # still waiting for a lock and execute their last command. If it doesn't
        # succeed then it goes back self.block_2pl().
        to_wake = set()
        logger.info('Txn {} releasing locks'.format(txn))
        for site in range(1, 11):
            to_wake = to_wake.union(set(self._sites[site].unlock(txn.tid)))

        logger.info('Txn {} lock release to wake {}'.format(txn, to_wake))

        del self._cur_txns[tid]
        self.tick()
        self.unblock_2pl(to_wake)
        if ws:
            self._recover_by_write(ws)

    def abort(self, tid):
        self._cur_txns[tid].abort_dl()
        self.finish_txn(tid)

    def unblock_2pl(self, to_wake):
        old_set = self._blocked_2pl.copy()
        self._blocked_2pl = set()

        for blocked in old_set:
            #logger.critical('blocked {} is {} in {}'.format(blocked, blocked[0], to_wake))
            if blocked[0] in to_wake:
                if len(blocked) == 3: # write
                    self.write(*blocked) #, recover_use_site=True)
                elif len(blocked) == 2:
                    self.read(*blocked)
            else:
                self._blocked_2pl.add(blocked)

    def read(self, tid, var):
        """
            Finds an available site to read. Checks if the site is up and if
            it's a replicated variable located on site n. Makes sure that the
            variable and site are available and returns one of them for reading.
            If not possible, adds the transaction to list of transactions that
            are blocked because sites have failed. block_2pl() holds
            transactions that are blocked due to two phase locking.
        """
        site = self._sites.find_available(var)
        if site is None:
            logger.info('Transaction {} fail blocked trying to read x{}'.format(
                self._cur_txns[tid], var))
            #if self._blocked_failed.index((tid) < 0:
            self._blocked_failed.add((tid, var))
            if self._full_output:
                print('T{} blocked reading x{} (no site)'.format(tid, var))
            return self.tick()

        txn = self._cur_txns[tid]

        if txn.read_only:
            """
                Checks if a transaction is read only. If so, then reads at the
                correct site and passes the transaction. Next, calls read() and
                tracks the sites where it's read. The asssert statement causes
                it to crash if mval is none.
            """
            mval = self._sites[site].read(var, txn)
            print('x{}: {}{}'.format(
                var, mval.value,
                ' (T{})'.format(tid) if self._full_output else ''))
            txn.read(var, mval, site)
            logger.info('Transaction {} read x{} at site {} value {} version {}'
                        .format(self._cur_txns[tid], var, site, *mval))
            assert mval is not None, 'Readonly should not fail'
            return self.tick()

        # mval is a named tuple which holds value and version. Defined in Sites.py.
        mval = self._sites[site].read(var, txn)

        if mval is None:
            #self._blocked_2pl.add((tid, var, site))
            self._blocked_2pl.add((tid, var))
            logger.info('Transaction {} blocked reading x{} at site {}'
                        .format(self._cur_txns[tid], var, site))
            if self._full_output:
                print('T{} blocked reading x{} (no lock)'.format(tid, var))
            return self.tick()

        print('x{}: {}{}'.format(
            var, mval.value,
            ' (T{})'.format(tid) if self._full_output else ''))
        txn.read(var, mval, site)
        logger.info('Transaction {} read x{} at site {} value {} version {}'
                    .format(self._cur_txns[tid], var, site, *mval))
        self.tick()

    def write(self, tid, var, value): # , recover_use_site=False):
        """
            Works the same way as read(). Gets all available sites. If Sites
            returns an empty array (no sites available), adds item as blocked.
            Tries to acquire every site it needs. Calls write_lock(). If return fail,
            add sites to need_locks(). Then need_locks() calls blocked_2pl().
        """
        sites = self._sites.find_available(var, all=True)
        txn = self._cur_txns[tid]

        # TODO: What would cause this to fail?
        if not sites:
            logger.info('Transaction {} fail blocked trying to write x{}'.format(
                self._cur_txns[tid], var))
            self._blocked_failed.add((tid, var, value))
            if self._log_writes:
                print('T{} blocked writing x{} (no site)'.format(tid, var))
            return self.tick()


        need_locks = []
        for s in sites:
            # Must acquire locks first
            locked = self._sites[s].write_lock(var, txn)
            if not locked:
                need_locks.append(s)

        if need_locks:
            # NOTE: Method for dealing with test case 15 (and similar cases)
            # where a transaction looking to write, fails to
            # acquire the locks of newly revived sites. In this case the sites
            # can be seen as still down sense they have not been written to
            if (self._test15_optimization and len(need_locks) != len(sites) and
                    all(self._sites[s][var].failed for s in need_locks)):
                for s in need_locks:
                    self._sites[s]._lm.leave_q(var, tid)
            else:
                self._blocked_2pl.add((tid, var, value))
                logger.info('Transaction {} blocked writing x{} at sites {}'
                            .format(txn, var, need_locks))
                if self._log_writes:
                    print('T{} blocked writing x{} (need locks)'.format(tid, var))
                return self.tick()


        # Success
        if self._log_writes:
            print('x{} = {} (T{})'.format(var, value, tid))
        txn.write(var, value, sites)
        logger.info('Transaction {} to write x{} at sites {} value {} version {}'
                    .format(self._cur_txns[tid], var, sites, value,
                            self._cur_txns[tid].timestamp))

        self.tick()

    def dump(self, var=None, site=None):
        """
            Gives the committed values of all copies of all variables at all
            sites, sorted per site.
        """
        assert (var is None or site is None), 'One argument must be None'
        sites = list(range(1, 11) if site is None else [site])
        vars_ = list(range(1, 21) if var is None else [var])
        for s in sites:
            str_base = 'site {} - '.format(s)
            v_strs = []
            for v in vars_:
                val = self._sites[s].bypass_failed(v)
                if val:
                    # Bypass site entry failed also
                    v_strs.append('x{}: {}'.format(v, val._values[0].value))
            if v_strs:
                print(str_base + ' '.join(v_strs))

    def fail(self, site):
        logger.info('Site {} failing'.format(site))
        self._sites[site].fail()
        for txn in self._cur_txns.values():
            txn.fail_site(site)

    def _recover_by_write(self, evens):
        old_set = self._blocked_failed.copy()
        self._blocked_failed = set()

        for blocked in old_set:
            #logger.critical('blocked {} is {} in {}'.format(blocked, blocked[0], to_wake))
            if len(blocked) == 2 and blocked[1] in evens:
                logger.info('Unblocking {} because of writes to {}'.format(blocked, evens))
                self.read(*blocked)
            else:
                self._blocked_2pl.add(blocked)

    def recover(self, site):
        logger.info('Site {} recovering'.format(site))
        self._sites[site].recover()
        logger.info('BLOCKED QUEUE: {}'.format(self._blocked_failed))

        old_set = self._blocked_failed.copy()
        self._blocked_failed = set()

        for blocked in old_set:
            #logger.critical('blocked {} is {} in {}'.format(blocked, blocked[0], to_wake))
            if blocked[1] % 2 == 0: # All sites will have even valued vars
                if len(blocked) == 3: # write
                    self.write(*blocked)
                elif len(blocked) == 2:
                    # Need to wait for a write...
                    self._blocked_failed.add(blocked)
            elif (blocked[1] + 1) % 10 == site:
                if len(blocked) == 3: # write
                    self.write(*blocked)
                elif len(blocked) == 2:
                    # Only copy, no need to block
                    self.read(*blocked)
            else:
                self._blocked_2pl.add(blocked)

    def youngest(self, path):
        youngest = None
        for item in path:
            txn = self._cur_txns[item]
            if youngest is None or youngest[1] < txn.timestamp:
                youngest = (txn.tid, txn.timestamp)
        logger.info('DL detect abort triggered for txn {} on path {}'.format(youngest[0], path))
        return youngest[0]

    # Need these functions: self._sites = Database(), youngest()
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


# JL
class TestDatabase(unittest.TestCase):
    """
        Unit tests for class Database
    """
    def setUp(self):
        self.db = Database()
        self.s = Site(1)
        self.s.failed

    def test__getitem__(self):
        with self.assertRaises(ValueError):
            self.db.__getitem__(0)
        with self.assertRaises(ValueError):
            self.db.__getitem__(11)

    def test__setitem__(self):
        with self.assertRaises(ValueError):
            self.db.__setitem__((12))
        with self.assertRaises(ValueError):
            self.db.__setitem__((0))

# JL
class TestTM(unittest.TestCase):
    """
        Unit tests for class TransactionManager
    """
    def setUp(self):
        self._tm = TransactionManager()

    def test_init(self):
        self.assertEqual(len(self._tm._sites._sites), 10)

    def test_time(self):
        self.assertEqual(self._tm.time, (1))

    def test_dl_detec(self):
        pass


if __name__ == '__main__':
    unittest.main()
