import logging

log = logging.getLogger(__name__)


def pytest_exception_interact(node, call, report):
    if hasattr(node, "_testcase") and hasattr(node._testcase, "cluster"):
        cluster = node._testcase.cluster
        log.error("Zookeeper cluster logs:")
        for logs in cluster.get_logs():
            log.error(logs)
