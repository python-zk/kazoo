import os
import sys


def test(arg):
    return os.system('bin/nosetests -s -d -v %s' % arg)


def main(args):
    if not args:
        print("Run as bin/python run_failure.py <test>, for example: \n"
            "bin/python run_failure.py "
            "kazoo.tests.test_watchers:KazooChildrenWatcherTests")
        return
    arg = args[0]
    while 1:
        ret = test(arg)
        if ret != 0:
            break


if __name__ == '__main__':
    main(sys.argv[1:])
