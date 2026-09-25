#!/bin/sh
# Generate the bench's private CA and the certificate the `tls` proxy serves.
# Runs in the `tls-certs` one-shot service on every `up`, so the host needs no
# openssl of its own. Files already there are kept while the server certificate
# names the proxy's address and is more than a day from expiring.
#
#   public/ca.pem       the CA, mounted into nodes and read by host recipes
#   server/server.pem   the proxy's certificate and key
#   ca/ca.key           the CA key, mounted nowhere
set -eu
proxy=10.97.25.25
cd "${1:-/tls}"
mkdir -p public server ca

if [ -s public/ca.pem ] && [ -s server/server.key ] &&
    openssl x509 -checkend 86400 -noout -in server/server.pem >/dev/null 2>&1 &&
    openssl x509 -checkip "$proxy" -noout -in server/server.pem 2>/dev/null | grep -q "does match"; then
    echo "bench TLS certificates present"
    exit 0
fi

openssl req -x509 -newkey ec -pkeyopt ec_paramgen_curve:prime256v1 -nodes \
    -keyout ca/ca.key -out public/ca.pem -days 3650 -subj "/CN=open-weave bench CA" \
    -addext "basicConstraints=critical,CA:TRUE" \
    -addext "keyUsage=critical,keyCertSign,cRLSign"

openssl req -newkey ec -pkeyopt ec_paramgen_curve:prime256v1 -nodes \
    -keyout server/server.key -out server/server.csr -subj "/CN=open-weave bench tls"
cat > server/server.ext <<EXT
basicConstraints=CA:FALSE
keyUsage=critical,digitalSignature
extendedKeyUsage=serverAuth
subjectAltName=IP:$proxy,IP:127.0.0.1,DNS:localhost
EXT
openssl x509 -req -in server/server.csr -CA public/ca.pem -CAkey ca/ca.key \
    -CAcreateserial -out server/server.pem -days 365 -extfile server/server.ext
rm -f server/server.csr server/server.ext public/ca.srl ca/ca.srl
chmod 644 public/ca.pem server/server.pem
chmod 600 ca/ca.key server/server.key
echo "generated a bench CA and server certificate"
