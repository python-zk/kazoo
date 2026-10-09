# JSSE keylog agent provisioner for the kazoo integration-test suite.
#
# On the `tls` auth flavor with `capture` active, the ensemble JVMs are
# launched with `-javaagent:/agent/extract-tls-secrets.jar=<keylog path>` so
# that TLS session secrets are exported in SSLKEYLOGFILE format, letting the
# captured pcapng artifacts be decrypted with the emitted keylog.
#
# This image downloads the **pinned, checksum-verified** `extract-tls-secrets`
# 5.0.0 agent jar at build time (exactly like `apk add tshark` in the capture
# image — never at session runtime) and runs as a one-shot sidecar that copies
# the jar into the shared ${KAZOO_TESTING_ZK_WORK_DIR}/agent mount and touches a `.ready`
# marker. The ZooKeeper servers mount the jar read-only at
# /agent/extract-tls-secrets.jar and their healthcheck/dependency on this
# service guarantees the jar exists before any JVM starts.
#
# Supply-chain discipline: the version (5.0.0) and SHA-256
# (015418eaf3…) are pinned here so the agent environment is reproducible and
# auditable, mirroring the pinned base images and `apk` packages. The SHA-256
# is verified at build time via `sha256sum -c`; a download/checksum failure
# aborts the image build, which the capture preflight surfaces as an
# actionable error before the stack starts.

FROM alpine:3.20

ENV AGENT_SRC=/agent-src
ENV AGENT_DIR=/agent

# Download the pinned agent jar at build time and verify its checksum. The
# `&&` chain guarantees the image is never left with an untrusted jar.
RUN mkdir -p /agent-src \
    && wget -q -O /agent-src/extract-tls-secrets.jar \
      https://repo1.maven.org/maven2/name/neykov/extract-tls-secrets/5.0.0/extract-tls-secrets-5.0.0.jar \
    && echo "015418eaf3ac0832909296af67fa3ec5149c53a075ead6cb29460b17db331ab0  /agent-src/extract-tls-secrets.jar" \
       | sha256sum -c -

COPY entrypoint.sh /usr/local/bin/tls-secrets-agent-entrypoint.sh

RUN chmod 755 /usr/local/bin/tls-secrets-agent-entrypoint.sh

ENTRYPOINT ["/usr/local/bin/tls-secrets-agent-entrypoint.sh"]
