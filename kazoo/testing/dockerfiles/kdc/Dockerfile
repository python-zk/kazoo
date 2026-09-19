# Test Kerberos KDC for the kazoo integration-test suite.
#
# An Alpine-based KDC (krb5kdc) that provisions a throwaway realm on every
# fresh start:
#   * writes /kdc-data/krb5.conf (KRB5_CONFIG)
#   * initializes the principal database (kdb5_util create -s)
#   * creates the SPNs in $SPNS and exports one keytab per SPN into
#     /kdc-data/keytabs with '/' mapped to '#' in the filename
#     (e.g. server/zoo1 -> server#zoo1.keytab)
#   * runs krb5kdc in the foreground
#
# /kdc-data is shared with the host via a ${KAZOO_TESTING_ZK_WORK_DIR} bind mount so the
# zoo servers can mount the keytabs and the host test process can read them
#

FROM alpine:3.20

# krb5-server provides krb5kdc/kdb5_util/kadmin.local; tini reaps signal
# forwarding for the foreground krb5kdc.
RUN apk add --no-cache krb5 krb5-server tini

# The entrypoint runs as root: /kdc-data is a bind mount of the host-owned
# ${KAZOO_TESTING_ZK_WORK_DIR} (created by pytest with 0700 host perms), so an unprivileged
# UID inside the container could not create the principal DB/keytabs there.
# The KDC is a throwaway test sidecar, and the entrypoint explicitly
# chmods its outputs world-readable for the host-side test process.
COPY --chown=daemon:daemon root/ /

RUN chmod 755 /entrypoint.sh

VOLUME /kdc-data
ENV SPNS="client server/zoo1 server/zoo2 server/zoo3"
ENV REALM="EXAMPLE.ORG"
ENV KDC_PORT=1088
ENV KDC_SERVICE="kdc"
EXPOSE 1088

ENTRYPOINT ["/sbin/tini", "--", "/entrypoint.sh"]
