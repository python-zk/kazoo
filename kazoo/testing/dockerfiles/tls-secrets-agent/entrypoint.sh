#!/bin/sh
#
# Install the pinned JSSE keylog agent jar into the shared agent bind mount and
# signal readiness for the compose healthcheck.
#
# Output:
#   $AGENT_DIR/extract-tls-secrets.jar   the agent jar, world-readable (the
#                                        ZooKeeper servers mount it read-only
#                                        at /agent/extract-tls-secrets.jar)
#   $AGENT_DIR/.ready                    marker file for the healthcheck
#
# The ZooKeeper servers mount ${KAZOO_TESTING_ZK_WORK_DIR}/agent read-only and depend on this
# service becoming healthy, which guarantees the `-javaagent:` jar exists
# before any JVM starts.

set -eu

AGENT_SRC="${AGENT_SRC:-/agent-src}"
AGENT_DIR="${AGENT_DIR:-/agent}"

mkdir -p "${AGENT_DIR}"
cp -f "${AGENT_SRC}/extract-tls-secrets.jar" "${AGENT_DIR}/extract-tls-secrets.jar"
chmod 644 "${AGENT_DIR}/extract-tls-secrets.jar"

# ---- Healthcheck marker --------------------------------------------------
touch "${AGENT_DIR}/.ready"
chmod 644 "${AGENT_DIR}/.ready"

echo "tls-secrets-agent: installed extract-tls-secrets.jar to ${AGENT_DIR}"
ls -l "${AGENT_DIR}"

# Stay alive so the healthcheck (`.ready` marker) can transition the container
# to `healthy` and `depends_on: condition: service_healthy` on the zoo nodes
# can proceed. Exiting here would leave the container in the `exited` state,
# which never becomes healthy.
while :; do
    sleep 3600
done
