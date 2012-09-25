import random


class HostIterator(object):
    """An iterator that returns selected hosts in order.

    A host is guaranteed to not be selected twice unless there is only
    one host in the collection.
    """

    def __init__(self, hosts):
        self.hosts = hosts

    def __iter__(self):
        for host in self.hosts[:]:
            yield host

    def __len__(self):
        return len(self.hosts)


class RandomHostIterator(HostIterator):
    """An iterator that returns a randomly selected host."""

    def __iter__(self):
        hostslist = self.hosts[:]
        random.shuffle(hostslist)
        for host in hostslist:
            yield host


def collect_hosts(hosts, randomize=True):
    """Collect a set of hosts and an optional chroot from a string."""
    host_ports, chroot = hosts.partition("/")[::2]
    chroot = "/" + chroot if chroot else None

    result = []
    for host_port in host_ports.split(","):
        host, port = host_port.partition(":")[::2]
        port = int(port.strip()) if port else 2181
        result.append((host.strip(), port))
    if randomize:
        return (RandomHostIterator(result), chroot)
    return (HostIterator(result), chroot)
