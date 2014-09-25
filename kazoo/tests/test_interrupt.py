import os

from kazoo.testing import KazooTestCase


class KazooLockTests(KazooTestCase):
    def test_interrupted_systemcall(self):
        '''
        Make sure interrupted system calls don't break the world, since we can't
        control what all signals our connection thread will get
        '''
        path = 'interrupt_test'
        self.client.create(path, b"1")

        # set the euid to the current process' euid. glibc sends SIGRT to all children
        os.seteuid(os.geteuid())

        self.client.get_children(path)
