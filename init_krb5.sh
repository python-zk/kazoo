#!/bin/bash

set -e

KRB5KDC=$(which krb5kdc || true)
KDB5_UTIL=$(which kdb5_util || true)
KADMIN=$(which kadmin.local || true)

if [ $# -lt 2 ]; then
    echo "Usage $0 TARGET_DIR CMD..."
    exit 1
fi
WRK_DIR=$1
shift

# Check installed packages
if [ -z ${KRB5KDC:+x} ] || [ -z ${KDB5_UTIL:+x} ] || [ -z ${KADMIN:+x} ]; then
    echo "Missing Kerberos utilities, skipping environment setup."
    exec $@
fi

if [ -e ${WRK_DIR} ]; then
    echo "Working directory kdc already exists!"
    exit 1
fi

WRK_DIR=$(readlink -f ${WRK_DIR})
KDC_DIR="${WRK_DIR}/krb5kdc"

###############################################################################
# Cleanup handlers

function kdclogs {
    echo "Kerberos environment logs:"
    tail -v -n50 ${KDC_DIR}/*.log
}

function killkdc {
    if [ -e ${KDC_DIR}/kdc.pid ]; then
        echo "Terminating KDC server listening on ${KDC_PORT}..."
        kill -TERM $(cat ${KDC_DIR}/kdc.pid)
    fi
    rm -vfr ${WRK_DIR}
}
trap killkdc EXIT
trap kdclogs ERR

###############################################################################
export KRB5_TEST_ENV=${WRK_DIR}
export KRB5_CONFIG=${WRK_DIR}/krb5.conf

KDC_PORT=$((${RANDOM}+1024))
mkdir -vp ${WRK_DIR}
mkdir -vp ${KDC_DIR}

cat <<EOF >${WRK_DIR}/krb5.conf
[logging]
 default = FILE:${KDC_DIR}/krb5libs.log
 kdc = FILE:${KDC_DIR}/krb5kdc.log
 admin_server = FILE:${KDC_DIR}/kadmind.log

[libdefaults]
 dns_lookup_realm = false
 ticket_lifetime = 24h
 renew_lifetime = 7d
 forwardable = true
 rdns = false
 default_realm = KAZOOTEST.ORG
 default_tkt_enctypes=aes128-cts-hmac-sha1-96
 default_tgs_enctypes=aes128-cts-hmac-sha1-96
 #default_ccache_name = KEYRING:persistent:%{uid}

[realms]
 KAZOOTEST.ORG = {
  database_name = ${KDC_DIR}/principal
  admin_keytab = FILE:${KDC_DIR}/kadm5.keytab
  key_stash_file = ${KDC_DIR}/stash
  kdc_listen = 127.0.0.1:${KDC_PORT}
  kdc_tcp_listen = 127.0.0.1:${KDC_PORT}
  kdc = 127.0.0.1:${KDC_PORT}
  kdc_ports = ${KDC_PORT}
  kdc_tcp_ports = ""
  default_domain = KAZOOTEST.ORG
 }

[domain_realm]
 .kazootest.org = KAZOOTEST.ORG
 kazootest.org = KAZOOTEST.ORG
EOF

cat <<EOF | ${KDB5_UTIL} create -s
passwd123
passwd123
EOF

cat <<EOF | ${KADMIN}
add_principal -randkey client@KAZOOTEST.ORG
ktadd -k ${WRK_DIR}/client.keytab -norandkey client@KAZOOTEST.ORG
add_principal -randkey zookeeper/127.0.0.1@KAZOOTEST.ORG
ktadd -k ${WRK_DIR}/server.keytab -norandkey zookeeper/127.0.0.1@KAZOOTEST.ORG
quit
EOF

# Starting KDC
echo "Starting KDC listening on ${KDC_PORT}..."
KRB5_KDC_PROFILE=${KRB5_CONFIG} ${KRB5KDC} \
    -P ${KDC_DIR}/kdc.pid \
    -p ${KDC_PORT} \
    -r KAZOOTEST.ORG

# Execute the next command
$@
