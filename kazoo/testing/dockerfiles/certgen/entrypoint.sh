#!/bin/sh
#
# Generate a throwaway CA + server/client TLS credentials into $CERTS_DIR.
#
# Output layout (see the Dockerfile header):
#   $CERTS_DIR/
#   ├── cacert.pem                 # CA certificate
#   ├── .ready                     # marker file for the healthcheck
#   ├── server/
#   │   ├── server.key             # server private key
#   │   ├── server.pem             # server cert
#   │   ├── keystore.p12           # key + cert as PKCS12 for ZooKeeper
#   │   └── truststore.p12         # CA cert as PKCS12 for ZooKeeper
#   └── client/
#       ├── client.key             # client private key
#       ├── client.pem             # client key + cert bundle (for kazoo)
#       └── cacert.pem             # CA cert (for kazoo's `ca` argument)
#
# All files are made world-readable so the host-side test process (any UID)
# can consume them from the shared ${KAZOO_TESTING_ZK_WORK_DIR}/certs bind mount.
# Nothing is ever written outside $CERTS_DIR.

set -eu

CERTS_DIR="${CERTS_DIR:-/certs}"
KEYSTORE_PASS="${KEYSTORE_PASS:-changeit}"
DAYS="${DAYS:-3650}"
SUBJECT_CA="/CN=kazoo-test-ca"
SUBJECT_SERVER="/CN=localhost"
SUBJECT_CLIENT="/CN=kazoo-client"

mkdir -p "${CERTS_DIR}/server" "${CERTS_DIR}/client"
cd "${CERTS_DIR}"

# ---- Certificate authority ---------------------------------------------
openssl req -x509 -newkey rsa:2048 -days "${DAYS}" -nodes \
    -keyout ca.key -out cacert.pem \
    -subj "${SUBJECT_CA}" 2>/dev/null

# ---- Server certificate (signed by the CA) ------------------------------
openssl req -newkey rsa:2048 -days "${DAYS}" -nodes \
    -keyout server/server.key -out server/server.csr \
    -subj "${SUBJECT_SERVER}" 2>/dev/null
openssl x509 -req -in server/server.csr -CA cacert.pem -CAkey ca.key \
    -CAcreateserial -out server/server.pem -days "${DAYS}" \
    -extfile /dev/stdin <<'EOF'
subjectAltName=DNS:localhost,IP:127.0.0.1
EOF

# keystore.p12: server key + cert (what ZooKeeper presents to clients)
openssl pkcs12 -export \
    -in server/server.pem -inkey server/server.key \
    -out server/keystore.p12 \
    -name zookeeper \
    -passout "pass:${KEYSTORE_PASS}"

# truststore.p12: the CA cert (what ZooKeeper validates client certs with)
keytool -importcert -noprompt \
    -alias ca \
    -file cacert.pem \
    -keystore server/truststore.p12 \
    -storetype PKCS12 \
    -storepass "${KEYSTORE_PASS}"

# ---- Client certificate (signed by the CA) ------------------------------
openssl req -newkey rsa:2048 -days "${DAYS}" -nodes \
    -keyout client/client.key -out client/client.csr \
    -subj "${SUBJECT_CLIENT}" 2>/dev/null
openssl x509 -req -in client/client.csr -CA cacert.pem -CAkey ca.key \
    -CAcreateserial -out client/client.crt -days "${DAYS}"

# kazoo loads the cert and key via a single certfile; bundle key+cert (PEM).
{
    cat client/client.key
    cat client/client.crt
} > client/client.pem
cp cacert.pem client/cacert.pem

# ---- Permissions: everything readable by the host test process ----------
chmod 755 "${CERTS_DIR}" "${CERTS_DIR}/server" "${CERTS_DIR}/client"
chmod 644 \
    cacert.pem ca.key \
    server/server.key server/server.pem server/keystore.p12 server/truststore.p12 \
    client/client.key client/client.pem client/client.crt client/cacert.pem

# ---- Healthcheck marker --------------------------------------------------
touch "${CERTS_DIR}/.ready"
chmod 644 "${CERTS_DIR}/.ready"

echo "certgen: wrote TLS credentials to ${CERTS_DIR}"
ls -l "${CERTS_DIR}" "${CERTS_DIR}/server" "${CERTS_DIR}/client"

# Stay alive so the healthcheck (`.ready` marker) can transition the container
# to `healthy` and `depends_on: condition: service_healthy` on the zoo nodes
# can proceed. Exiting here would leave the container in the `exited` state,
# which never becomes healthy.
while :; do
    sleep 3600
done
