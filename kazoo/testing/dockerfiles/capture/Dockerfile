# In-repo capture image: tshark on Alpine.
#
# The host needs no capture tooling; everything runs here, inside a member's
# network namespace, mirroring the KDC/certgen pattern.
#
# ARCHITECTURE NOTE: the overlay runs one capture sidecar *per ensemble
# member* (`zooN-capture`), each joining its member's network namespace
# (`network_mode: service:zooN`, T011) and capturing on the member's own
# eth0 NON-promiscuously (-p). This is deterministic on Docker Desktop (whose
# userland bridge never mirrors cross-container unicast onto a peer's veth)
# and on native Linux alike, because the capture simply sees the traffic that
# reaches the interface the ZooKeeper JVM itself listens on — no bridge
# participation at all. The network-namespace holder (zooN, see
# docker-compose.base.yml) keeps the netns — and therefore this tap — alive
# across `zooN-service` stop/start cycles, so failure-injection tests never
# interrupt the capture; a fresh unique-named output file per
# invocation keeps the collection lossless if the sidecar container itself is
# ever recreated (see capture-entrypoint.sh).
#
# ENTRYPOINT is the capture-entrypoint.sh wrapper, which appends the `-w`
# output path; the overlay's `command:` is therefore flags-only and must NOT
# include `-w`. The first `command:` token is the member name, consumed by the
# wrapper for the filename (do not prefix `command:` with `tshark` — compose
# appends `command` to `ENTRYPOINT`, which would double-execute the binary).

FROM alpine:3.20

RUN apk add --no-cache tshark

COPY capture-entrypoint.sh /usr/local/bin/capture-entrypoint.sh
RUN chmod +x /usr/local/bin/capture-entrypoint.sh

ENTRYPOINT ["/usr/local/bin/capture-entrypoint.sh"]