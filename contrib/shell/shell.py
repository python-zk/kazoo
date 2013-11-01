# -*- coding: utf-8 -*-
#
# a zkCli.sh clone - though not everything is supported currently
# It supports the basic ops:
#
#  python contrib/shell.py localhost:2181
#  (CONNECTED) /> ls
#  zookeeper
#  (CONNECTED) /> create foo 'bar'
#  (CONNECTED) /> get foo
#  bar
#  (CONNECTED) /> cd foo
#  (CONNECTED) /foo> create ish 'barish'
#  (CONNECTED) /foo> cd ..
#  (CONNECTED) /> ls foo
#  ish
#  (CONNECTED) /> create temp- 'temp' true true
#  (CONNECTED) /> ls
#  zookeeper foo temp-0000000001
#  (CONNECTED) /> rmr foo
#  (CONNECTED) />
#  (CONNECTED) /> tree
#  .
#  ├── zookeeper
#  │   ├── config
#  │   ├── quota


from __future__ import print_function

import argparse
import cmd
from collections import defaultdict
import os
import re
import shlex
import sys

from kazoo.client import KazooClient
from kazoo.exceptions import NotEmptyError
from kazoo.protocol.states import EventType
from kazoo.security import make_acl, make_digest_acl

from copy import copy, CopyError

class ShellParser(argparse.ArgumentParser):
    def error(self, message):
        raise Exception(message)


class Shell(cmd.Cmd):
    curdir = '/'

    def __init__(self, hosts=[]):
        cmd.Cmd.__init__(self)
        self._hosts = hosts
        self._setup_readline()

        self._zk = None
        self._read_only = False
        self.connected = False
        self._watching_paths = {}

        if len(self._hosts) > 0:
            self._connect(self._hosts)

        if not self.connected:
            self._update_curdir('/')

    def run(self):
        self.cmdloop("")

    def connected(f):
        def wrapped(self, args):
          if self.connected:
                return f(self, args)
          print("Not connected.")
        return wrapped

    def ensure_params(expected_params):
        def wrapper(f):
            parser = ShellParser()
            for p, optional in expected_params:
                if optional is True:
                    parser.add_argument(p)
                elif optional is False:
                    parser.add_argument(p, nargs="?", default="")
                elif optional is "+":
                    parser.add_argument(p, nargs="+")

            def wrapped(self, args):
                try:
                    params = parser.parse_args(shlex.split(args))
                    return f(self, params)
                except Exception as ex:
                    valid_params = " ".join(
                        e[0] if e[1] else "<%s>" % (e[0]) for e in expected_params)
                    print("Wrong params: %s. Expected: %s" % (str(ex), valid_params))
            return wrapped
        return wrapper

    def check_path_exists(f):
        def wrapped(self, params):
            path = params.path
            params.path = self._abspath(path if path != '' else self.curdir)
            if self._zk.exists(params.path):
                return f(self, params)
            print("Path %s doesn't exist" % (path))
        return wrapped

    def check_path_absent(f):
        def wrapped(self, params):
            path = params.path
            params.path = self._abspath(path if path != '' else self.curdir)
            if not self._zk.exists(params.path):
                return f(self, params)
            print("Path %s already exists" % (path))
        return wrapped

    @connected
    @ensure_params([("scheme", True), ("credential", True)])
    def do_add_auth(self, params):
        self._zk.add_auth(params.scheme, params.credential)

    def help_add_auth(self):
        print("""
allows you to authenticate your session.

example:
  add_auth digest super:s3cr3t
""")

    class BadACL(Exception): pass

    @connected
    @ensure_params([("path", True), ("acls", "+")])
    @check_path_exists
    def do_set_acls(self, params):
        acls = self._extract_acls(params.acls)
        try:
            self._zk.set_acls(params.path, acls)
        except Exception as ex:
            print("Failed to set ACLs: %s. Error: %s" % (str(acls), str(ex)))

    def help_set_acls(self):
        print("""
sets ACLs for a given path.

example:
  set_acls /some/path world:anyone:r digest:user:aRxISyaKnTP2+OZ9OmQLkq04bvo=:cdrwa
  set_acls /some/path world:anyone:r username_password:user:p@ass0rd:cdrwa
""")

    def _extract_acls(self, acls):
        return map(self._extract_acl, acls)

    valid_schemes = [
        "world",
        "auth",
        "digest",
        "host",
        "ip",
        "username_password",  # internal-only: gen digest from user:password
    ]

    def _extract_acl(self, acl):
        try:
            scheme, rest = acl.split(":", 1)
            credential = ":".join(rest.split(":")[0:-1])
            cdrwa = rest.split(":")[-1]
        except ValueError:
            raise self.BadACL("Bad ACL: %s. Format is scheme:id:perms" % (acl))

        if scheme not in self.valid_schemes:
            raise self.BadACL("Invalid scheme: %s" % (acl))

        create = True if "c" in cdrwa else False
        read = True if "r" in cdrwa else False
        write = True if "w" in cdrwa else False
        delete = True if "d" in cdrwa else False
        admin = True if "a" in cdrwa else False

        if scheme == "username_password":
            username, password = credential.split(":", 1)
            return make_digest_acl(username, password, read, write, create,
                                   delete, admin)
        else:
            return make_acl(scheme, credential, read, write,
                            create, delete, admin)

    @connected
    @ensure_params([("path", True)])
    @check_path_exists
    def do_get_acls(self, params):
        print(self._zk.get_acls(params.path)[0])

    def help_get_acls(self):
        print("""
gets ACLs for a given path.

example:
  get_acls /zookeeper
  [ACL(perms=31, acl_list=['ALL'], id=Id(scheme=u'world', id=u'anyone'))]
""")

    @connected
    @ensure_params([("path", False), ("watch", False)])
    @check_path_exists
    def do_ls(self, params):
        if params.watch.lower() == "true":
            znodes = self._zk.get_children(params.path, watch=self._default_watcher)
        else:
            znodes = self._zk.get_children(params.path)
        print(" ".join(znodes))

    def complete_ls(self, cmd_param_text, full_cmd, start_idx, end_idx):
        return self._complete_path(cmd_param_text, full_cmd)

    @connected
    @ensure_params([("command", True),
                    ("path", True)])
    @check_path_exists
    def do_watch(self, params):
        if params.command == "start":
            if params.path in self._watching_paths:
                print("%s is already being watched" % (params.path))
                return

            self._watching_paths[params.path] = defaultdict(int)
            self._set_watches(params.path)
        elif params.command == "stop":
            if params.path not in self._watching_paths:
                print("%s is not being watched" % (params.path))
                return
            del self._watching_paths[params.path]
        elif params.command == "stats":
            if params.path not in self._watching_paths:
                print("%s is not being watched" % (params.path))
                return

            print("\nWatches Stats\n")
            for path, count in self._watching_paths[params.path].items():
                print("%s: %d" % (path, count))
        else:
            print("watch <start|stop> <path> [verbose]")

    def _set_watches(self, path):
        for c in self._zk.get_children(path, self._watches_stats_watcher):
            self._set_watches("%s/%s" % (path, c))

    def _watches_stats_watcher(self, watched_event):
        try:
            if watched_event.type != EventType.CHILD:
                return

            for path, stats in self._watching_paths.items():
                if watched_event.path.startswith(path):
                    stats[watched_event.path] += 1

            self._zk.get_children(watched_event.path,
                                  self._watches_stats_watcher)
        except Exception as ex:
            print(str(ex))

    @ensure_params([("src", True), ("dst", True),
                    ("recursive", False), ("overwrite", False),
                    ("verbose", False)])
    def do_cp(self, params):
        try:
            recursive = params.recursive.lower() == "true"
            overwrite = params.overwrite.lower() == "true"
            verbose = params.verbose.lower() == "true"
            copy(params.src, params.dst, recursive, overwrite, verbose)
        except CopyError as ex:
            print(str(ex))

    def help_cp(self):
        print("""
copy from/to local/remote or remote/remote paths.

example:
  cp file://<path> zk://[user:passwd@]host/<path> <recursive> <overwrite> <verbose>
""")

    @connected
    @ensure_params([("path", False)])
    @check_path_exists
    def do_tree(self, params):
        print(".")
        self._print_tree(params.path, 0)

    def complete_tree(self, cmd_param_text, full_cmd, start_idx, end_idx):
        return self._complete_path(cmd_param_text, full_cmd)

    def _print_tree(self, path, indent):
        for c in self._zk.get_children(path):
            print(u"%s├── %s" % (u"│   " * indent, c))
            self._print_tree(u"%s/%s" % (path, c), indent + 1)

    @connected
    @ensure_params([("path", True)])
    @check_path_exists
    def do_cd(self, params):
        self._update_curdir(params.path)

    def complete_cd(self, cmd_param_text, full_cmd, start_idx, end_idx):
        return self._complete_path(cmd_param_text, full_cmd)

    @connected
    @ensure_params([("path", True), ("watch", False)])
    @check_path_exists
    def do_get(self, params):
        if params.watch.lower() == "true":
            value, stat = self._zk.get(params.path, watch=self._default_watcher)
        else:
            value, stat = self._zk.get(params.path)
        print(value)

    def complete_get(self, cmd_param_text, full_cmd, start_idx, end_idx):
        return self._complete_path(cmd_param_text, full_cmd)

    def help_get(self):
        print("""
gets the value for a given znode. a watch can be set.

example:
  get /foo
  bar

  # sets a watch
  get /foo true

  # trigger the watch
  set /foo 'notbar'
  WatchedEvent(type='CHANGED', state='CONNECTED', path=u'/foo')
""")

    @connected
    @ensure_params([("path", True), ("watch", False)])
    @check_path_exists
    def do_exists(self, params):
        if params.watch.lower() == "true":
            stat = self._zk.exists(params.path, watch=self._default_watcher)
        else:
            stat = self._zk.exists(params.path)
        print(stat)

    def complete_exists(self, cmd_param_text, full_cmd, start_idx, end_idx):
        return self._complete_path(cmd_param_text, full_cmd)

    def help_exists(self):
        print("""
checks if path exists and returns the stat for the znode. a watch can be set.

example:
  exists /foo
  ZnodeStat(czxid=101, mzxid=102, ctime=1382820644375, mtime=1382820693801, version=1, cversion=0, aversion=0, ephemeralOwner=0, dataLength=6, numChildren=0, pzxid=101)

  # sets a watch
  exists /foo true

  # trigger the watch
  rm /foo
  WatchedEvent(type='DELETED', state='CONNECTED', path=u'/foo')
""")

    def _default_watcher(self, watched_event):
        print((str(watched_event)))

    @connected
    @ensure_params([("path", True),
                    ("value", True),
                    ("ephemeral", False),
                    ("sequence", False),
                    ("recursive", False)])
    @check_path_absent
    def do_create(self, params):
        ephemeral = params.ephemeral.lower() == "true"
        sequence = params.sequence.lower() == "true"
        makepath = params.recursive.lower() == "true"
        self._zk.create(params.path,
                        params.value,
                        acl=None,
                        ephemeral=ephemeral,
                        sequence=sequence,
                        makepath=makepath)

    def complete_create(self, cmd_param_text, full_cmd, start_idx, end_idx):
        return self._complete_path(cmd_param_text, full_cmd)

    def help_create(self):
        print("""
creates a znode in a given path. it can also be ephemeral and/or sequential. it can also be created recursively.

example:
  create /foo 'bar'

  # create an ephemeral znode
  create /foo1 '' true

  # create an ephemeral|sequential znode
  create /foo1 '' true true

  # recursively create a path
  create /very/long/path/here '' false false true

  # check the new subtree
  tree
  .
  ├── zookeeper
  │   ├── config
  │   ├── quota
  ├── very
  │   ├── long
  │   │   ├── path
  │   │   │   ├── here
""")

    @connected
    @ensure_params([("path", True), ("value", True)])
    @check_path_exists
    def do_set(self, params):
        self._zk.set(params.path, params.value)

    def complete_set(self, cmd_param_text, full_cmd, start_idx, end_idx):
        return self._complete_path(cmd_param_text, full_cmd)

    def help_set(self):
        print("""
sets the value for a znode.

example:
  set /foo 'bar'
""")

    @connected
    @ensure_params([("path", True)])
    @check_path_exists
    def do_rm(self, params):
        try:
            self._zk.delete(params.path)
        except NotEmptyError:
            print("%s is not empty." % (path))

    def complete_rm(self, cmd_param_text, full_cmd, start_idx, end_idx):
        return self._complete_path(cmd_param_text, full_cmd)

    @connected
    @ensure_params([("path", True)])
    @check_path_exists
    def do_rmr(self, params):
        self._delete_recursive(params.path)

    def _delete_recursive(self, path):
        for c in self._zk.get_children(path):
            child_path = os.path.join(path, c)
            self._delete_recursive(child_path)

        self._zk.delete(path)

    def complete_rmr(self, cmd_param_text, full_cmd, start_idx, end_idx):
        return self._complete_path(cmd_param_text, full_cmd)

    def help_rmr(self):
        print("""
recursively deletes a path.

example:
  rmr /foo
""")

    @connected
    @ensure_params([("path", True)])
    @check_path_exists
    def do_sync(self, params):
        self._zk.sync(params.path)

    @ensure_params([("hosts", True)])
    def do_connect(self, params):
        self._connect(params.hosts.split(","))

    def help_connect(self):
        print("""
connects to a host from a list of hosts given.

example:
  connect host1:2181,host2:2181
""")

    @connected
    def do_disconnect(self, args):
        self._disconnect()
        self._update_curdir('/')

    def help_disconnect(self):
        print("""
disconnects from the currently connected host.

example:
  disconnect
""")

    @connected
    def do_pwd(self, args):
        print("%s" % (self.curdir))

    def do_EOF(self, *args):
        self._exit(True)

    def do_quit(self, *args):
        self._exit(False)

    def do_exit(self, *args):
        self._exit(False)

    def _disconnect(self):
        if self._zk:
            try:
                self._zk.stop()
            except:
                pass

    def _connect(self, hosts):
        self._disconnect()
        self._zk = KazooClient(hosts=",".join(hosts), read_only=self._read_only)
        self._zk.start()
        self.connected = True
        self._update_curdir('/')

    def _update_curdir(self, dirpath):
        if dirpath == '..':
            if self.curdir == '/':
                dirpath = '/'
            else:
                dirpath = os.path.dirname(self.curdir)
        elif not dirpath.startswith('/'):
            prefix = self.curdir
            if prefix != '/':
                prefix += '/'
            dirpath = prefix + dirpath

        if dirpath != '/' and not self._zk.exists(dirpath):
            print("Path %s doesn't exist." % dirpath)
        else:
            self.curdir = dirpath
            self.prompt = "(%s) %s> " % (self._state(), dirpath)

    def _state(self):
        if self._zk:
            return self._zk.state
        return "DISCONNECTED"

    def _exit(self, newline=True):
        if newline:
            print("")
        sys.exit(0)

    def _abspath(self, path):
        if path == '..':
            return os.path.dirname(self.curdir)
        elif path.startswith('/'):
            return path
        elif self.curdir == '/':
            return "/%s" % (path)
        else:
            return "%s/%s" % (self.curdir, path)

    def _setup_readline(self):
        try:
            import readline
            import atexit
        except ImportError:
            return

        histfile = os.path.join(os.environ['HOME'], '.kz-shell-history')
        try:
            readline.read_history_file(histfile)
        except IOError:
            pass
        atexit.register(readline.write_history_file, histfile)

    def _complete_path(self, cmd_param_text, full_cmd):
        pieces = shlex.split(full_cmd)
        cmd_param = pieces[1] if len(pieces) > 1 else cmd_param_text
        offs = len(cmd_param) - len(cmd_param_text)
        path = cmd_param[:-1] if cmd_param.endswith("/") else cmd_param

        if re.match("^\s*$", path):
            return self._zk.get_children(self.curdir)

        if self._zk.exists(path):
            opts = map(lambda z: "%s/%s" % (path, z),
                       self._zk.get_children(self._abspath(path)))
        elif "/" not in path:
            znodes = self._zk.get_children(self.curdir)
            opts = filter(lambda z: z.startswith(path), znodes)
        else:
            parent = os.path.dirname(path)
            child = os.path.basename(path)
            opts = map(lambda z: "%s/%s" % (parent, z),
                       filter(lambda z: z.startswith(child),
                              self._zk.get_children(parent)))

        return map(lambda x: x[offs:], opts)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("hosts", nargs="*", help="ZK hosts to connect")
    params = parser.parse_args()

    try:
        Shell(params.hosts).run()
    except KeyboardInterrupt:
        print("\n")
