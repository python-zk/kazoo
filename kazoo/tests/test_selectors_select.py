"""
The official python select function test case copied from python source
 to test the selector_select function.
"""

import os
import socket
import sys
import unittest

from kazoo.handlers.utils import selector_select

select = selector_select


@unittest.skipIf(
    (sys.platform[:3] == "win"), "can't easily test on this system"
)
class SelectTestCase(unittest.TestCase):
    class Nope:
        pass

    class Almost:
        def fileno(self):
            return "fileno"

    def test_error_conditions(self):
        self.assertRaises(TypeError, select, 1, 2, 3)
        self.assertRaises(TypeError, select, [self.Nope()], [], [])
        self.assertRaises(TypeError, select, [self.Almost()], [], [])
        self.assertRaises(TypeError, select, [], [], [], "not a number")
        self.assertRaises(ValueError, select, [], [], [], -1)

    # Issue #12367: http://www.freebsd.org/cgi/query-pr.cgi?pr=kern/155606
    @unittest.skipIf(
        sys.platform.startswith("freebsd"),
        "skip because of a FreeBSD bug: kern/155606",
    )
    def test_errno(self):
        with open(__file__, "rb") as fp:
            fd = fp.fileno()
            fp.close()
            self.assertRaises(ValueError, select, [fd], [], [], 0)

    def test_returned_list_identity(self):
        # See issue #8329
        r, w, x = select([], [], [], 1)
        self.assertIsNot(r, w)
        self.assertIsNot(r, x)
        self.assertIsNot(w, x)

    def test_select(self):
        cmd = "for i in 0 1 2 3 4 5 6 7 8 9; do echo testing...; sleep 1; done"
        p = os.popen(cmd, "r")
        for tout in (0, 1, 2, 4, 8, 16) + (None,) * 10:
            rfd, wfd, xfd = select([p], [], [], tout)
            if (rfd, wfd, xfd) == ([], [], []):
                continue
            if (rfd, wfd, xfd) == ([p], [], []):
                line = p.readline()
                if not line:
                    break
                continue
            self.fail("Unexpected return values from select():", rfd, wfd, xfd)
        p.close()

    # Issue 16230: Crash on select resized list
    def test_select_mutated(self):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            a = []

            class F:
                def fileno(self):
                    del a[-1]
                    return s.fileno()

            a[:] = [F()] * 10
            self.assertEqual(select([], a, []), ([], a[:5], []))


if __name__ == "__main__":
    unittest.main()
