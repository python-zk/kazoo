#!/bin/sh
# Capture entrypoint (delivers the tshark image a command).
#
# The capture service's compose `command:` is flags-only; this
# wrapper appends a uniquely-named `-w` output file so that a sidecar restart
# (e.g. an engine or holder restart recreating the container) never overwrites
# a previous capture. The artifact is intentionally a *collection* of
# per-member pcapng files — the filenames don't matter, only the set does.
#
# Lifecycle note: the sidecar lives in the member's network namespace, owned by
# the network-holder service (zooN). Stopping/starting the ZooKeeper process
# (zooN-service) does NOT restart this container — the holder keeps the netns
# (and this tap) alive across the member's downtime (see
# docker-compose.base.yml header / features-capture.yml ARCHITECTURE NOTE).
#
# `tshark -w` cannot be given both here and in the compose `command:`, so -w is
# appended here and must NOT appear in the overlay's `command:`.
set -eu

# Nanosecond epoch (busybox `date +%s%N`), stable and unique per invocation;
# $1 carries the member name this sidecar belongs to (e.g. "zoo1").
name="${1-unknown}"
shift || true

output="/captures/kazoo-client-${name}-$(date +%s%N).pcapng"
echo "capture: writing to ${output}" >&2

exec tshark "$@" -w "${output}"