import uuid
import threading

from nose.tools import eq_, ok_, raises

from kazoo.exceptions import CancelledError
from kazoo.exceptions import LockTimeout
from kazoo.testing import KazooTestCase
from kazoo.tests.util import wait


class KazooLockTests(KazooTestCase):
    def setUp(self):
        super(KazooLockTests, self).setUp()
        self.lockpath = "/" + uuid.uuid4().hex

        self.condition = threading.Condition()
        self.released = threading.Event()
        self.active_thread = None
        self.cancelled_threads = []

    def _thread_lock_acquire_til_event(self, name, lock, event):
        try:
            with lock:
                with self.condition:
                    eq_(self.active_thread, None)
                    self.active_thread = name
                    self.condition.notify_all()

                event.wait()

                with self.condition:
                    eq_(self.active_thread, name)
                    self.active_thread = None
                    self.condition.notify_all()
            self.released.set()
        except CancelledError:
            with self.condition:
                self.cancelled_threads.append(name)
                self.condition.notify_all()

    def test_lock_one(self):
        lock_name = uuid.uuid4().hex
        lock = self.client.Lock(self.lockpath, lock_name)
        event = threading.Event()

        thread = threading.Thread(target=self._thread_lock_acquire_til_event,
            args=(lock_name, lock, event))
        thread.start()

        lock2_name = uuid.uuid4().hex
        anotherlock = self.client.Lock(self.lockpath, lock2_name)

        # wait for any contender to show up on the lock
        wait(anotherlock.contenders)
        eq_(anotherlock.contenders(), [lock_name])

        with self.condition:
            while self.active_thread != lock_name:
                self.condition.wait()

        # release the lock
        event.set()

        with self.condition:
            while self.active_thread:
                self.condition.wait()
        self.released.wait()
        thread.join()

    def test_exclusive_only_lock(self):
        threads = []
        names = ["contender" + str(i) for i in range(5)]

        contender_bits = {}

        for name in names:
            e = threading.Event()

            l = self.client.Lock(self.lockpath, name)
            t = threading.Thread(target=self._thread_lock_acquire_til_event,
                args=(name, l, e))
            contender_bits[name] = (t, e)
            threads.append(t)

        # acquire the lock ourselves first to make the others line up
        lock = self.client.Lock(self.lockpath, "test")
        lock.acquire()

        for t in threads:
            t.start()

        # wait for everyone to line up on the lock
        wait(lambda: len(lock.contenders()) == 6)
        contenders = lock.contenders()

        eq_(contenders[0], "test")
        contenders = contenders[1:]
        remaining = list(contenders)

        # release the lock and contenders should claim it in order
        lock.release()

        for contender in contenders:
            thread, event = contender_bits[contender]

            with self.condition:
                while not self.active_thread:
                    self.condition.wait()
                eq_(self.active_thread, contender)

            eq_(lock.contenders(), remaining)
            remaining = remaining[1:]

            event.set()

            with self.condition:
                while self.active_thread:
                    self.condition.wait()
        for thread in threads:
            thread.join()

    def test_lock_non_blocking(self):
        lock_name = uuid.uuid4().hex
        lock = self.client.Lock(self.lockpath, lock_name)
        event = threading.Event()

        thread = threading.Thread(target=self._thread_lock_acquire_til_event,
            args=(lock_name, lock, event))
        thread.start()

        lock1 = self.client.Lock(self.lockpath, lock_name)

        # wait for the thread to acquire the lock
        with self.condition:
            if not self.active_thread:
                self.condition.wait(5)

        ok_(not lock1.acquire(blocking=False))
        eq_(lock.contenders(), [lock_name])  # just one - itself

        event.set()
        thread.join()

    def test_lock_fail_first_call(self):
        event1 = threading.Event()
        lock1 = self.client.Lock(self.lockpath, "one")
        thread1 = threading.Thread(target=self._thread_lock_acquire_til_event,
            args=("one", lock1, event1))
        thread1.start()

        # wait for this thread to acquire the lock
        with self.condition:
            if not self.active_thread:
                self.condition.wait(5)
                eq_(self.active_thread, "one")
        eq_(lock1.contenders(), ["one"])
        event1.set()
        thread1.join()

    def test_lock_cancel(self):
        event1 = threading.Event()
        lock1 = self.client.Lock(self.lockpath, "one")
        thread1 = threading.Thread(target=self._thread_lock_acquire_til_event,
            args=("one", lock1, event1))
        thread1.start()

        # wait for this thread to acquire the lock
        with self.condition:
            if not self.active_thread:
                self.condition.wait(5)
                eq_(self.active_thread, "one")

        client2 = self._get_client()
        client2.start()
        event2 = threading.Event()
        lock2 = client2.Lock(self.lockpath, "two")
        thread2 = threading.Thread(target=self._thread_lock_acquire_til_event,
            args=("two", lock2, event2))
        thread2.start()

        # this one should block in acquire. check that it is a contender
        wait(lambda: len(lock2.contenders()) > 1)
        eq_(lock2.contenders(), ["one", "two"])

        lock2.cancel()
        with self.condition:
            if not "two" in self.cancelled_threads:
                self.condition.wait()
                assert "two" in self.cancelled_threads

        eq_(lock2.contenders(), ["one"])

        thread2.join()
        event1.set()
        thread1.join()
        client2.stop()

    def test_lock_double_calls(self):
        lock1 = self.client.Lock(self.lockpath, "one")
        lock1.acquire()
        lock1.acquire()
        lock1.release()
        lock1.release()

    def test_lock_reacquire(self):
        lock = self.client.Lock(self.lockpath, "one")
        lock.acquire()
        lock.release()
        lock.acquire()
        lock.release()

    def test_lock_timeout(self):
        timeout = 3
        e = threading.Event()
        started = threading.Event()

        # In the background thread, acquire the lock and wait thrice the time
        # that the main thread is going to wait to acquire the lock.
        lock1 = self.client.Lock(self.lockpath, "one")

        def _thread(lock, event, timeout):
            with lock:
                started.set()
                event.wait(timeout)
                if not event.isSet():
                    # Eventually fail to avoid hanging the tests
                    self.fail("lock2 never timed out")

        t = threading.Thread(target=_thread, args=(lock1, e, timeout * 3))
        t.start()

        # Start the main thread's kazoo client and try to acquire the lock
        # but give up after `timeout` seconds
        client2 = self._get_client()
        client2.start()
        started.wait(5)
        self.assertTrue(started.isSet())
        lock2 = client2.Lock(self.lockpath, "two")
        try:
            lock2.acquire(timeout=timeout)
        except LockTimeout:
            # A timeout is the behavior we're expecting, since the background
            # thread should still be holding onto the lock
            pass
        else:
            self.fail("Main thread unexpectedly acquired the lock")
        finally:
            # Cleanup
            e.set()
            t.join()
            client2.stop()


class KazooSharedLockTests(KazooTestCase):
    def setUp(self):
        super(KazooSharedLockTests, self).setUp()
        self.lockpath = "/" + uuid.uuid4().hex
        self.condition = threading.Condition()
        self.active_threads = []
        self.cancelled_threads = []
        self.callback_threads = []

    def test_all_exclusive_locks(self):
        self._do_test(contender_count=10, exclusive_mod=1)

    def test_all_shared_locks(self):
        self._do_test(contender_count=10, exclusive_mod=0)

    def test_mixture_shared_exclusive_locks(self):
        self._do_test(contender_count=10, exclusive_mod=3)

    def test_revocations_published(self):
        self._do_test(contender_count=10, exclusive_mod=2, revocation_mod=4)

    def test_revocations_call_back(self):
        self._do_test(contender_count=10, exclusive_mod=2, revocation_mod=4,
                      callback_mod=3)

    @raises(ValueError)
    def test_reserved_word_shared(self):
        self.client.Lock(self.lockpath + "__SHARED__")

    @raises(ValueError)
    def test_reserved_word_exclusive(self):
        self.client.Lock(self.lockpath + "__EXCLUSIVE__")

    @raises(ValueError)
    def test_reserved_word_unlock(self):
        self.client.Lock(self.lockpath, "identifier __UNLOCK__")

    def _do_test(self, contender_count, exclusive_mod=0,
                 revocation_mod=0, callback_mod=0,
                 callback_action_mod=0):
        """Runs a shared lock test based on the parameters given. Allows
        testing different combinations of shared vs exclusive locks, locks that
        request revocations, and locks that receive callbacks. This is useful
        given the range of lock strategies offered by ``Lock``.

        The method computes the expected groups of locks that should occur
        given the parameters. It then tests the state of the lock system at
        each of these phases, such as which locks have been acquired, which
        are pending, whether revocations have been issued, and whether a single
        revocation callback has been received for impacted locks.

        The main parameter is ``contender_count``, which indicates how many
        concurrent lock contenders with associated threads will be created. The
        remaining parameters indicate the modulus interval (of the contender
        identifier) that will attract a particular behavior. For example, a
        ``contender_count`` of 20 with an ``exclusive_mod`` of 6 will produce
        lock contenders numbered 0 to 19, and contenders numbered 0, 6, 12 and
        18 will request exclusive locks. Use modulus 0 to disable a behaviour.

        :param contender_count: number of contenders to create
        :param exclusive_mod: the modulus that will be used to produce
                              exclusive locks (use 1 to make all contenders use
                              exclusive locks, or 0 for all contenders to use
                              shared locks)
        :param revocation_mod: the modulus that will request revocation of any
                               existing lock
        :param callback_mod: the modulus that will request an unlock callback

        """
        # record the lock contenders we're creating
        def build_contenders(contender_count, modulus):
            if modulus == 0:
                return []
            return ["contender" + str(i) for i in range(contender_count)
                    if i % modulus == 0]

        names = build_contenders(contender_count, 1)
        exclusives = build_contenders(contender_count, exclusive_mod)
        revokers = build_contenders(contender_count, revocation_mod)
        callbacks = build_contenders(contender_count, callback_mod)

        # compute the phases (ie groups of concurrent locks) that should occur
        phases = []
        this_phase = []
        for name in names:
            if name in exclusives:
                if len(this_phase) > 0:
                    phases.append(this_phase)  # store last phase
                phases.append([name])  # record a phase with just the exclusive
                this_phase = []  # start new phase
            else:
                this_phase.append(name)  # shared
        phases.append(this_phase)  # store the final phase

        # setup our contenders
        threads = []
        contender_bits = {}
        for name in names:
            e = threading.Event()

            exclusive = True if name in exclusives else False
            l = self.client.Lock(self.lockpath, name, exclusive=exclusive)

            revoke = True if name in revokers else False
            callback = True if name in callbacks else False
            revoke = True if name in revokers else False
            t = threading.Thread(target=self._thread_lock_acquire_til_event,
                                 args=(name, l, e, revoke, callback))
            contender_bits[name] = (t, e)
            threads.append(t)

        # acquire the lock ourselves first to make the others line up
        lock = self.client.Lock(self.lockpath, "test")
        lock.acquire()

        # start threads one-by-one to prevent thread scheduling inconsistencies
        for contender in names:
            thread, event = contender_bits[contender]
            thread.start()
            wait(lambda: contender in lock.contenders())

        contenders = lock.contenders()
        eq_(contenders[0], "test")
        contenders = contenders[1:]
        remaining = list(contenders)

        # release the lock and contenders should claim it in order
        lock.release()

        # validate state at each phase
        for phase in phases:
            for contender in phase:
                thread, event = contender_bits[contender]

                with self.condition:
                    while not contender in self.active_threads:
                        self.condition.wait()

            # all claim to have their locks, so check expected remainders
            eq_(lock.contenders(), remaining)
            remaining = [x for x in remaining if x not in phase]

            unlocks = lock.contenders(unlocks_only=True)
            if not set(remaining).isdisjoint(revokers):
                # 1+ revoker is remaining, so revocations should have been made
                # for at least all members of this phase
                ok_(set(phase).issubset(set(unlocks)))

                # in addition all members of this phase with callbacks should
                # have fired
                for expected_callback in set(phase).intersection(callbacks):
                    with self.condition:
                        while not expected_callback in self.callback_threads:
                            self.condition.wait()

            # release their locks
            for contender in phase:
                thread, event = contender_bits[contender]
                event.set()

                with self.condition:
                    while contender in self.active_threads:
                        self.condition.wait()

        # ensure all terminate
        for thread in threads:
            thread.join()

    def _thread_lock_acquire_til_event(self, name, lock, event, revoke,
                                       listener):
        unlock = None
        if listener:
            def unlock_callback():
                with self.condition:
                    # ensures callback only fired once
                    ok_(name not in self.callback_threads)
                    self.callback_threads.append(name)
                    self.condition.notify_all()
            unlock = unlock_callback

        try:
            if lock.acquire(revoke=revoke, unlock=unlock):
                # record we have a lock
                with self.condition:
                    ok_(name not in self.active_threads)
                    self.active_threads.append(name)
                    self.condition.notify_all()

                event.wait()

                # record we're releasing our lock
                with self.condition:
                    ok_(name in self.active_threads)
                    self.active_threads.remove(name)
                    self.condition.notify_all()
            else:
                raise AssertionError('{} never acquired lock'.format(name))
        except CancelledError:
            with self.condition:
                self.cancelled_threads.append(name)
                self.condition.notify_all()
        finally:
            lock.cancel()
            lock.release()


class TestSemaphore(KazooTestCase):
    def setUp(self):
        super(TestSemaphore, self).setUp()
        self.lockpath = "/" + uuid.uuid4().hex

        self.condition = threading.Condition()
        self.released = threading.Event()
        self.active_thread = None
        self.cancelled_threads = []

    def test_basic(self):
        sem1 = self.client.Semaphore(self.lockpath)
        sem1.acquire()
        sem1.release()

    def test_lock_one(self):
        sem1 = self.client.Semaphore(self.lockpath, max_leases=1)
        sem2 = self.client.Semaphore(self.lockpath, max_leases=1)
        started = threading.Event()
        event = threading.Event()

        sem1.acquire()

        def sema_one():
            started.set()
            with sem2:
                event.set()

        thread = threading.Thread(target=sema_one, args=())
        thread.start()
        started.wait(10)

        self.assertFalse(event.is_set())

        sem1.release()
        event.wait(10)
        self.assert_(event.is_set())
        thread.join()

    def test_non_blocking(self):
        sem1 = self.client.Semaphore(
            self.lockpath, identifier='sem1', max_leases=2)
        sem2 = self.client.Semaphore(
            self.lockpath, identifier='sem2', max_leases=2)
        sem3 = self.client.Semaphore(
            self.lockpath, identifier='sem3', max_leases=2)

        sem1.acquire()
        sem2.acquire()
        ok_(not sem3.acquire(blocking=False))
        eq_(set(sem1.lease_holders()), set(['sem1', 'sem2']))
        sem2.release()
        # the next line isn't required, but avoids timing issues in tests
        sem3.acquire()
        eq_(set(sem1.lease_holders()), set(['sem1', 'sem3']))
        sem1.release()
        sem3.release()

    def test_non_blocking_release(self):
        sem1 = self.client.Semaphore(
            self.lockpath, identifier='sem1', max_leases=1)
        sem2 = self.client.Semaphore(
            self.lockpath, identifier='sem2', max_leases=1)
        sem1.acquire()
        sem2.acquire(blocking=False)

        # make sure there's no shutdown / cleanup error
        sem1.release()
        sem2.release()

    def test_holders(self):
        started = threading.Event()
        event = threading.Event()

        def sema_one():
            with self.client.Semaphore(self.lockpath, 'fred', max_leases=1):
                started.set()
                event.wait()

        thread = threading.Thread(target=sema_one, args=())
        thread.start()
        started.wait()
        sem1 = self.client.Semaphore(self.lockpath)
        holders = sem1.lease_holders()
        eq_(holders, ['fred'])
        event.set()
        thread.join()

    def test_semaphore_cancel(self):
        sem1 = self.client.Semaphore(self.lockpath, 'fred', max_leases=1)
        sem2 = self.client.Semaphore(self.lockpath, 'george', max_leases=1)
        sem1.acquire()
        started = threading.Event()
        event = threading.Event()

        def sema_one():
            started.set()
            try:
                with sem2:
                    started.set()
            except CancelledError:
                event.set()

        thread = threading.Thread(target=sema_one, args=())
        thread.start()
        started.wait()
        eq_(sem1.lease_holders(), ['fred'])
        eq_(event.is_set(), False)
        sem2.cancel()
        event.wait()
        eq_(event.is_set(), True)
        thread.join()

    def test_multiple_acquire_and_release(self):
        sem1 = self.client.Semaphore(self.lockpath, 'fred', max_leases=1)
        sem1.acquire()
        sem1.acquire()

        eq_(True, sem1.release())
        eq_(False, sem1.release())

    def test_handle_session_loss(self):
        expire_semaphore = self.client.Semaphore(self.lockpath, 'fred',
                                                 max_leases=1)

        client = self._get_client()
        client.start()
        lh_semaphore = client.Semaphore(self.lockpath, 'george', max_leases=1)
        lh_semaphore.acquire()

        started = threading.Event()
        event = threading.Event()
        event2 = threading.Event()

        def sema_one():
            started.set()
            with expire_semaphore:
                event.set()
                event2.wait()

        thread = threading.Thread(target=sema_one, args=())
        thread.start()

        started.wait()
        eq_(lh_semaphore.lease_holders(), ['george'])

        # Fired in a separate thread to make sure we can see the effect
        expired = threading.Event()

        def expire():
            self.expire_session()
            expired.set()

        thread = threading.Thread(target=expire, args=())
        thread.start()
        expire_semaphore.wake_event.wait()
        expired.wait()

        lh_semaphore.release()
        client.stop()

        event.wait(5)
        eq_(expire_semaphore.lease_holders(), ['fred'])
        event2.set()
        thread.join()

    def test_inconsistent_max_leases(self):
        sem1 = self.client.Semaphore(self.lockpath, max_leases=1)
        sem2 = self.client.Semaphore(self.lockpath, max_leases=2)

        sem1.acquire()
        self.assertRaises(ValueError, sem2.acquire)

    def test_inconsistent_max_leases_other_data(self):
        sem1 = self.client.Semaphore(self.lockpath, max_leases=1)
        sem2 = self.client.Semaphore(self.lockpath, max_leases=2)

        self.client.ensure_path(self.lockpath)
        self.client.set(self.lockpath, b'a$')

        sem1.acquire()
        # sem2 thinks it's ok to have two lease holders
        ok_(sem2.acquire(blocking=False))

    def test_reacquire(self):
        lock = self.client.Semaphore(self.lockpath)
        lock.acquire()
        lock.release()
        lock.acquire()
        lock.release()

    def test_acquire_after_cancelled(self):
        lock = self.client.Semaphore(self.lockpath)
        self.assertTrue(lock.acquire())
        self.assertTrue(lock.release())
        lock.cancel()
        self.assertTrue(lock.cancelled)
        self.assertTrue(lock.acquire())

    def test_timeout(self):
        timeout = 3
        e = threading.Event()
        started = threading.Event()

        # In the background thread, acquire the lock and wait thrice the time
        # that the main thread is going to wait to acquire the lock.
        sem1 = self.client.Semaphore(self.lockpath, "one")

        def _thread(sem, event, timeout):
            with sem:
                started.set()
                event.wait(timeout)
                if not event.isSet():
                    # Eventually fail to avoid hanging the tests
                    self.fail("sem2 never timed out")

        t = threading.Thread(target=_thread, args=(sem1, e, timeout * 3))
        t.start()

        # Start the main thread's kazoo client and try to acquire the lock
        # but give up after `timeout` seconds
        client2 = self._get_client()
        client2.start()
        started.wait(5)
        self.assertTrue(started.isSet())
        sem2 = client2.Semaphore(self.lockpath, "two")
        try:
            sem2.acquire(timeout=timeout)
        except LockTimeout:
            # A timeout is the behavior we're expecting, since the background
            # thread will still be holding onto the lock
            e.set()
        finally:
            # Cleanup
            t.join()
            client2.stop()
