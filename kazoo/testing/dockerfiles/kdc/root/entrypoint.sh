#!/bin/sh
#
# Test Kerberos KDC bootstrap + foreground krb5kdc.
#
# Writes into /kdc-data (shared with the host via a ${KAZOO_TESTING_ZK_WORK_DIR} bind
# mount):
#   /kdc-data/krb5.conf           realm/KD C config (KRB5_CONFIG)
#   /kdc-data/krb5kdc/            principal database
#   /kdc-data/logs/               log files
#   /kdc-data/keytabs/            one keytab per SPN ('/' -> '#' in filename)
#                                 plus a combined server.keytab
#
# Everything is created world-readable so the host-side test process (any
# UID) and the zoo containers can consume it. The KDC listens
# on 0.0.0.0:${KDC_PORT} so the zoo containers can reach it at
# ${KDC_SERVICE}:${KDC_PORT} over the compose network.

set -e

KDC_SERVICE="${KDC_SERVICE:-kdc}"

WRK_DIR="/kdc-data"
KDC_DIR="${WRK_DIR}/krb5kdc"
LOG_DIR="${WRK_DIR}/logs"
KTB_DIR="${WRK_DIR}/keytabs"

mkdir -p "${KDC_DIR}" "${LOG_DIR}" "${KTB_DIR}"

export KRB5_CONFIG="${WRK_DIR}/krb5.conf"

KDC_PORT="${KDC_PORT:-1088}"
SPNS="${SPNS:-client server/zoo1 server/zoo2 server/zoo3}"
REALM="${REALM:-EXAMPLE.ORG}"
DOMAIN="${DOMAIN:-$(printf '%s' "${REALM}" | tr '[:upper:]' '[:lower:]')}"

cat <<EOF >"${KRB5_CONFIG}"
[logging]
 default = FILE:${LOG_DIR}/krb5libs.log
 kdc = FILE:${LOG_DIR}/krb5kdc.log
 admin_server = FILE:${LOG_DIR}/kadmind.log

[libdefaults]
 dns_lookup_realm = false
 ticket_lifetime = 24h
 renew_lifetime = 7d
 forwardable = true
 rdns = false
 default_realm = ${REALM}

[realms]
 ${REALM} = {
  database_name = ${KDC_DIR}/principal
  admin_keytab = FILE:${KDC_DIR}/kadm5.keytab
  key_stash_file = ${KDC_DIR}/stash
  kdc_listen = 0.0.0.0:${KDC_PORT}
  kdc_tcp_listen = 0.0.0.0:${KDC_PORT}
  kdc = ${KDC_SERVICE}:${KDC_PORT}
  default_domain = ${DOMAIN}
 }

[domain_realm]
 .${DOMAIN} = ${REALM}
 ${DOMAIN} = ${REALM}
EOF

# Create the principal database if it does not exist yet (idempotent across
# sidecar restarts on the shared /kdc-data volume).
if [ ! -f "${KDC_DIR}/principal" ]; then
    printf 'passwd123\npasswd123\n' | kdb5_util create -s
fi

for SPN in ${SPNS}; do
    # '/' in the SPN maps to '#' in the keytab filename (no subdirectories).
    KTFILE="${KTB_DIR}/$(printf '%s' "${SPN}" | tr '/' '#').keytab"
    # add_principal fails if the principal already exists; ignore that.
    kadmin.local -q "add_principal -randkey ${SPN}@${REALM}" >/dev/null 2>&1 || true
    kadmin.local -q "ktadd -k ${KTFILE} -norandkey ${SPN}@${REALM}"
done

# Service principal the kazoo client requests a ticket for: kazoo uses
# service "zookeeper" and the *host the client connects to*, which for the
# published ports is 127.0.0.1 (or localhost). Export those keys into the
# combined server keytab so any client host form resolves.
for CLIENT_HOST in 127.0.0.1 localhost; do
    kadmin.local -q "add_principal -randkey zookeeper/${CLIENT_HOST}@${REALM}" >/dev/null 2>&1 || true
    kadmin.local -q "ktadd -k ${KTB_DIR}/zookeeper#${CLIENT_HOST}.keytab -norandkey zookeeper/${CLIENT_HOST}@${REALM}"
done

# Combined server keytab: every server/* principal plus the client-reachable
# zookeeper/<host> forms. Each zoo server mounts this single file at
# /conf/server.keytab (see docker-compose.auth-sasl-gssapi.yml).
kadmin.local -q "ktadd -k ${KTB_DIR}/server.keytab \
zookeeper/127.0.0.1@${REALM} zookeeper/localhost@${REALM} \
server/zoo1@${REALM} server/zoo2@${REALM} server/zoo3@${REALM}"

# World-readable so the host test process and the zoo containers can read
# keytabs regardless of UID.
chmod 755 "${WRK_DIR}" "${KDC_DIR}" "${LOG_DIR}" "${KTB_DIR}"
chmod 644 "${KRB5_CONFIG}"
chmod 644 "${KTB_DIR}"/*.keytab

# Start the KDC in the foreground (PID file + port + realm from env).
echo "Starting KDC for ${REALM} on port ${KDC_PORT}..."
exec krb5kdc \
    -P "${KDC_DIR}/kdc.pid" \
    -p "${KDC_PORT}" \
    -r "${REALM}" \
    -n
