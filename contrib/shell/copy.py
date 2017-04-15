# helpers to move files/dirs to and from ZK and also among ZK clusters

from __future__ import print_function

from collections import defaultdict, namedtuple
import json
import os
import re
import urlparse

from kazoo.client import KazooClient


DEFAULT_ZK_PORT = 2181


def zk_client(host, username, password):
    if not re.match(":\d+$", host):
        hostname = "%s:%d" % (host, DEFAULT_ZK_PORT)

    client = KazooClient(hosts=host)
    client.start()
    return client


class Netloc(namedtuple("Netloc", "username password host")):
    @classmethod
    def from_string(cls, netloc_string):
        username = password = host = ""
        if not "@" in netloc_string:
            host = netloc_string
        else:
            username_passwd, host =  netloc.split("@")
            if ":" in username_passwd:
                username, password = username_passwd.split(":", 1)
            else:
                username = username_passwd

        return cls(username, password, host)


scheme_registry = {}
def scheme_handler(scheme):
    def class_wrapper(cls):
        scheme_registry[scheme] = cls
        return cls
    return class_wrapper


class CopyError(Exception): pass


class Proxy(object):

    def __init__(self, parse_result, exists):
        self.parse_result = parse_result
        self.netloc = Netloc.from_string(parse_result.netloc)
        self.exists = exists

    @property
    def scheme(self):
        return self.parse_result.scheme

    @property
    def url(self):
        return self.parse_result.geturl()

    @property
    def path(self):
        return self.parse_result.path

    @property
    def host(self):
        return self.netloc.host

    @property
    def username(self):
        return self.netloc.username

    @property
    def password(self):
        return self.netloc.password

    def set_url(self, string):
        """ useful for recycling a stateful proxy """
        self.parse_result = Proxy.parse(string)

    @classmethod
    def from_string(cls, string, exists):
        """
        if exists is bool, then check it either exists or it doesn't.
        if exists is None, we don't care.
        """
        result = cls.parse(string)

        if result.scheme not in scheme_registry:
            raise CopyError("Invalid scheme: %s" % (result.scheme))

        return scheme_registry[result.scheme](result, exists)

    @classmethod
    def parse(cls, url_string):
        """ default to file:// """
        if not re.match("^\w+://", url_string):
            url_string = "file://%s" % (url_string)

        return urlparse.urlparse(url_string)

    def __enter__(self):
        pass

    def __exit__(self, type, value, traceback):
        pass

    def check_path(self):
        raise NotImplementedError, "check_path must be implemented"

    def read_path(self):
        raise NotImplementedError, "read_path must be implemented"

    def write_path(self, content):
        raise NotImplementedError, "write_path must be implemented"

    def children_of(self):
        raise NotImplementedError, "children_of must be implemented"


@scheme_handler("zk")
class ZKProxy(Proxy):
    """ read/write ZooKeeper paths """

    def __init__(self, parse_result, exists):
        super(ZKProxy, self).__init__(parse_result, exists)

        self.client = zk_client(self.host, self.username, self.password)

        if exists is not None:
            self.check_path()

    def check_path(self):
        retval = True if self.client.exists(self.path) else False
        if retval is not self.exists:
            if self.exists:
                m = "znode %s in %s doesn't exist" % \
                    (self.path, self.host)
            else:
                m = "znode %s in %s exists" % (self.path, self.host)
            raise CopyError(m)

    def read_path(self):
        return self.client.get(self.path)[0]

    def write_path(self, content):
        if self.client.exists(self.path):
            self.client.set(self.path, content)
        else:
            self.client.create(self.path, content, makepath=True)

    def children_of(self):
        return self.zk_walk(self.path, "")

    def zk_walk(self, root_path, path):
        paths = []
        full_path = "%s/%s" % (root_path, path) if path != "" else root_path
        for c in self.client.get_children(full_path):
            child_path = "%s/%s" % (path, c) if path != "" else c
            paths.append(child_path)
            paths += self.zk_walk(root_path, child_path)
        return paths


@scheme_handler("file")
class FileProxy(Proxy):
    def __init__(self, parse_result, exists):
        super(FileProxy, self).__init__(parse_result, exists)

        if exists is not None:
            self.check_path()

    def check_path(self):
        if os.path.exists(self.path) is not self.exists:
            m = "Path %s " % (self.path)
            m += "doesn't exist" if self.exists else "exists"
            raise CopyError(m)

    def read_path(self):
        if os.path.isfile(self.path):
            with open(self.path, "r") as fp:
                return "".join(fp.readlines())
        elif os.path.isdir(self.path):
            return ""
        else:
            raise CopyError("%s is of unknown file type" % (self.path))

    def write_path(self, content):
        """ this will overwrite dst path - be careful """

        parent_dir = os.path.dirname(self.path)
        try:
            os.makedirs(parent_dir)
        except OSError as ex:
            pass
        with open(self.path, "w") as fp:
            fp.write(content)

    def children_of(self):
        root_path = self.path[0:-1] if self.path.endswith("/") else self.path
        all = []
        for path, dirs, files in os.walk(root_path):
            path = path.replace(root_path, "")
            if path.startswith("/"):
                path = path[1:]
            if path != "":
                all.append(path)
            for f in files:
                all.append("%s/%s" % (path, f) if path != "" else f)
        return all


@scheme_handler("json")
class JSONProxy(Proxy):
    """ read/write from JSON files discovered via:

          json://!some!path!backup.json/some/path

        the serialized version looks like this:

        .. code-block:: python

         {
          '/some/path': {
             'content': 'blob',
             'acls': []},
          '/some/other/path': {
             'content': 'other-blob',
             'acls': []},
         }

        For simplicity, a flat dictionary is used as opposed as
        using a tree like format with children accessible from
        their parent.
    """

    def __enter__(self):
        self._dirty = False  # tracks writes
        self._file_path = self.host.replace("!", "/")

        self._tree = defaultdict(dict)
        if os.path.exists(self._file_path):
            with open(self._file_path, "r") as fp:
                self._tree = json.load(fp)

        if self.exists is not None:
            self.check_path()

    def __exit__(self, type, value, traceback):
        if not self._dirty:
            return

        with open(self._file_path, "w") as fp:
            json.dump(self._tree, fp, indent=4)

    def check_path(self):
        if (self.path in self._tree) != self.exists:
            m = "Path %s " % (self.path)
            m += "doesn't exist" if self.exists else "exists"
            raise CopyError(m)

    def read_path(self):
        return self._tree[self.path]["content"].encode('utf-8')

    def write_path(self, content):
        self._tree[self.path]["content"] = content
        self._tree[self.path]["acls"] = []  # not implemented (yet)
        self._dirty = True

    def children_of(self):
        offs = 1 if self.path == "/" else len(self.path) + 1
        return map(lambda c: c[offs:],
                   filter(lambda k: k != self.path and k.startswith(self.path),
                          self._tree.keys()))


def do_copy(src, dst, verbose=False):
    if verbose:
        print("Copying from %s to %s" % (src.geturl(), dst.geturl()))

    try:
        dst.write_path(src.read_path())
    except Exception as ex:
        raise CopyError("Failed to copy: %s" % (str(ex)))


def url_join(url_root, child_path):
    if url_root.endswith("/"):
        return "%s%s" % (url_root, child_path)
    else:
        return "%s/%s" % (url_root, child_path)


def copy(src_url, dst_url, recursive=False, overwrite=False, verbose=False):
    """
       src and dst can be any of:

       file://<path>
       zk://[user:passwd@]host/<path>
       json://!some!path!backup.json/some/path

       with a few restrictions (i.e.: bare in mind the semantic differences
       that znodes have with filesystem directories - so recursive copying
       from znodes to an fs could lose data, but to a JSON file it would
       work just fine.
    """
    src = Proxy.from_string(src_url, True)
    dst = Proxy.from_string(dst_url, None if overwrite else False)

    # basic sanity check
    if recursive and src.scheme == "zk" and dst.scheme == "file":
        raise CopyError("Recursive copy from zk to fs isn't supported")

    with src, dst:
        do_copy(src, dst, verbose)
        if recursive:
            children = src.children_of()
            for c in children:
                src.set_url(url_join(src_url, c))
                dst.set_url(url_join(dst_url, c))
                do_copy(src, dst, verbose)
