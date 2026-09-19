from __future__ import annotations

import sys
import threading
import uuid

import pytest

from kazoo.tests.util import wait


class UniqueError(Exception):
    """Error raised only by test leader function"""


class TestKazooElection:
    def test_election(self, zkclient):
        path = "/" + uuid.uuid4().hex
        condition = threading.Condition()

        # election contenders set these when elected. The exit event is set by
        # the test to make the leader exit.
        leader_id = [None]
        exit_event = [None]

        # tests set this before the event to make the leader raise an error
        raise_exception = [False]

        # set by a worker thread when an unexpected error is hit.
        thread_exc_info = [None]

        def check_thread_error():
            if thread_exc_info[0]:
                t, o, tb = thread_exc_info[0]
                raise t(o)

        def spawn_contender(contender_id, election):
            thread = threading.Thread(
                target=election_thread, args=(contender_id, election)
            )
            thread.daemon = True
            thread.start()
            return thread

        def election_thread(contender_id, election):
            try:
                election.run(leader_func, contender_id)
            except UniqueError:
                if not raise_exception[0]:
                    thread_exc_info[0] = sys.exc_info()
            except Exception:
                thread_exc_info[0] = sys.exc_info()
            else:
                if raise_exception[0]:
                    e = Exception("expected leader func to raise exception")
                    thread_exc_info[0] = (Exception, e, None)

        def leader_func(name):
            ev = threading.Event()
            with condition:
                exit_event[0] = ev
                leader_id[0] = name
                condition.notify_all()

            ev.wait(45)
            if raise_exception[0]:
                raise UniqueError("expected error in the leader function")

        elections = {}
        threads = {}
        for _ in range(3):
            contender = "c" + uuid.uuid4().hex
            elections[contender] = zkclient.Election(path, contender)
            threads[contender] = spawn_contender(
                contender, elections[contender]
            )

        # wait for a leader to be elected
        times = 0
        with condition:
            while not leader_id[0]:
                condition.wait(5)
                times += 1
                if times > 5:
                    raise Exception(
                        "Still not a leader: lid: %s", leader_id[0]
                    )

        election = zkclient.Election(path)

        # make sure all contenders are in the pool
        wait(lambda: len(election.contenders()) == len(elections))
        contenders = election.contenders()

        assert set(contenders) == set(elections.keys())

        # first one in list should be leader
        first_leader = contenders[0]
        assert first_leader == leader_id[0]

        # tell second one to cancel election. should never get elected.
        elections[contenders[1]].cancel()

        # make leader exit. third contender should be elected.
        exit_event[0].set()
        with condition:
            while leader_id[0] == first_leader:
                condition.wait(45)
        assert leader_id[0] == contenders[2]
        check_thread_error()

        # make first contender re-enter the race
        threads[first_leader].join()
        threads[first_leader] = spawn_contender(
            first_leader, elections[first_leader]
        )

        # contender set should now be the current leader plus the first leader
        wait(lambda: len(election.contenders()) == 2)
        contenders = election.contenders()
        assert set(contenders) == {first_leader, leader_id[0]}

        # make current leader raise an exception. first should be reelected
        raise_exception[0] = True
        exit_event[0].set()
        with condition:
            while leader_id[0] != first_leader:
                condition.wait(45)
        assert leader_id[0] == first_leader
        check_thread_error()

        exit_event[0].set()
        for thread in threads.values():
            thread.join()
        check_thread_error()

    def test_bad_func(self, zkclient):
        path = "/" + uuid.uuid4().hex
        election = zkclient.Election(path)
        with pytest.raises(ValueError):
            election.run("not a callable")
