import random


class RandomHostIterator(object):
    """An iterator that returns a randomly selected host.

    A host is guaranteed to not be selected twice unless there is only
    one host in the collection.

    """
    def __init__(self, hosts):
        self.last = 0
        self.hosts = hosts

    def __iter__(self):
        hostslist = self.hosts[:]
        random.shuffle(hostslist)
        for host in hostslist:
            yield host

    def __len__(self):
        return len(self.hosts)


def collect_hosts(hosts):
    """Collect a set of hosts and an optional chroot from a string."""
    host_ports, chroot = hosts.partition("/")[::2]
    chroot = "/" + chroot if chroot else None

    result = []
    for host_port in host_ports.split(","):
        host, port = host_port.partition(":")[::2]
        port = int(port.strip()) if port else 2181
        result.append((host.strip(), port))
    return (RandomHostIterator(result), chroot)
