#!/bin/bash
set -e

# FROM: https://gist.github.com/komuw/076231fd9b10bb73e40f
export TARGET_DIR="certificates"
export DAYS=50000
export SSL_HOST="mysql-ssl"

# Cleanup
rm -rf $TARGET_DIR
mkdir $TARGET_DIR
cd $TARGET_DIR

# Create the CA Key and Certificate for signing Client Certs
openssl genrsa -out ca-key.pem 4096
openssl req -subj "/CN=ca" -new -x509 -days $DAYS -key ca-key.pem -out ca-cert.pem

# Create the Server Key, CSR, and Certificate
openssl genrsa -out server-key.pem 4096
openssl req -subj "/CN=${SSL_HOST}" -new -key server-key.pem -out server-cert.csr

# We're self signing our own server cert here.  This is a no-no in production.
openssl x509 -req -days $DAYS -in server-cert.csr -CA ca-cert.pem -CAkey ca-key.pem -set_serial 01 -out server-cert.pem

# Create the Client Key and CSR
openssl genrsa -out client-key.pem 4096
openssl req -subj "/CN=-client" -new -key client-key.pem -out client-cert.csr

# Sign the client certificate with our CA cert.  Unlike signing our own server cert, this is what we want to do.
# Serial should be different from the server one, otherwise curl will return NSS error -8054
openssl x509 -req -days $DAYS -in client-cert.csr -CA ca-cert.pem -CAkey ca-key.pem -set_serial 02 -out client-cert.pem

# Verify Server Certificate
openssl verify -purpose sslserver -CAfile ca-cert.pem server-cert.pem

# Verify Client Certificate
openssl verify -purpose sslclient -CAfile ca-cert.pem client-cert.pem
