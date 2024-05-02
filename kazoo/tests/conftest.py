import logging

log = logging.getLogger(__name__)


def pytest_exception_interact(node, call, report):
    try:
        cluster = node._testcase.cluster
        log.error("Zookeeper cluster logs:")
        for logs in cluster.get_logs():
            log.error(logs)
    except Exception:
        log.exception("Cannot get ZK logs:")
