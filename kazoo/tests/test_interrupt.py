import os
from nose import SkipTest
from sys import platform

from kazoo.testing import KazooTestCase


class KazooInterruptTests(KazooTestCase):
    def test_interrupted_systemcall(self):
        '''
        Make sure interrupted system calls don't break the world, since we can't
        control what all signals our connection thread will get
        '''
        if 'linux' not in platform:
            raise SkipTest('Unable to reproduce error case on non-linux platforms')

        path = 'interrupt_test'
        self.client.create(path, b"1")

        # set the euid to the current process' euid.
        # glibc sends SIGRT to all children, which will interrupt the system call
        os.seteuid(os.geteuid())

        self.client.get_children(path)
