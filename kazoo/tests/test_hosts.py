from unittest import TestCase
from kazoo.hosts import collect_hosts


class HostsTestCase(TestCase):

    def test_ipv4(self):
        hosts, chroot = collect_hosts('127.0.0.1:2181, 192.168.1.2:2181, \
                                       132.254.111.10:2181')
        self.assertEquals([('127.0.0.1', 2181),
                           ('192.168.1.2', 2181),
                           ('132.254.111.10', 2181)], hosts)
        self.assertEquals(None, chroot)

        hosts, chroot = collect_hosts(['127.0.0.1:2181',
                                       '192.168.1.2:2181',
                                       '132.254.111.10:2181'])
        self.assertEquals([('127.0.0.1', 2181),
                           ('192.168.1.2', 2181),
                           ('132.254.111.10', 2181)], hosts)
        self.assertEquals(None, chroot)

    def test_ipv6(self):
        hosts, chroot = collect_hosts('[fe80::200:5aee:feaa:20a2]:2181')
        self.assertEquals([('fe80::200:5aee:feaa:20a2', 2181)], hosts)
        self.assertEquals(None, chroot)

        hosts, chroot = collect_hosts(['[fe80::200:5aee:feaa:20a2]:2181'])
        self.assertEquals([('fe80::200:5aee:feaa:20a2', 2181)], hosts)
        self.assertEquals(None, chroot)

    def test_hosts_list(self):

        hosts, chroot = collect_hosts('zk01:2181, zk02:2181, zk03:2181')
        expected1 = [('zk01', 2181), ('zk02', 2181), ('zk03', 2181)]
        self.assertEquals(expected1, hosts)
        self.assertEquals(None, chroot)

        hosts, chroot = collect_hosts(['zk01:2181', 'zk02:2181', 'zk03:2181'])
        self.assertEquals(expected1, hosts)
        self.assertEquals(None, chroot)

        expected2 = '/test'
        hosts, chroot = collect_hosts('zk01:2181, zk02:2181, zk03:2181/test')
        self.assertEquals(expected1, hosts)
        self.assertEquals(expected2, chroot)

        hosts, chroot = collect_hosts(['zk01:2181',
                                       'zk02:2181',
                                       'zk03:2181', '/test'])
        self.assertEquals(expected1, hosts)
        self.assertEquals(expected2, chroot)
