# Ephemeral TLS credential generator for the kazoo integration-test suite.
#
# Runs as a one-shot sidecar (healthcheck: keystore exists) that writes a
# throwaway PKI into the shared ${KAZOO_TESTING_ZK_WORK_DIR}/certs mount:
#
#   certs/
#   ├── cacert.pem                 # CA certificate (world-readable)
#   ├── server/
#   │   ├── keystore.p12           # server key+cert chain (PKCS12)
#   │   └── truststore.p12         # CA cert as truststore (PKCS12)
#   └── client/
#       ├── client.pem             # client key+cert bundle (PEM, world-readable)
#       └── cacert.pem             # CA cert for the client's verify location
#
# The ZooKeeper servers mount keystore/truststore at /conf/keystore.p12 and
# /conf/truststore.p12; the host-side test process reads client creds from
# ${KAZOO_TESTING_ZK_WORK_DIR}/certs. Everything under /certs is chmod 755/644
# so the host can read it regardless of UID.
#
# eclipse-temurin is used because it ships both `openssl` (via the base image)
# and `keytool` (via the JRE — the JDK variant adds ~260MB for no benefit
# here), avoiding a separate openssl image.

FROM eclipse-temurin:17-jre-jammy

ENV CERTS_DIR=/certs
ENV KEYSTORE_PASS=changeit

COPY entrypoint.sh /usr/local/bin/certgen-entrypoint.sh

RUN chmod 755 /usr/local/bin/certgen-entrypoint.sh

ENTRYPOINT ["/usr/local/bin/certgen-entrypoint.sh"]
