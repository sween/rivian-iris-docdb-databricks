#!/bin/bash

cat << 'EOF' > /usr/local/share/ca-certificates/cloudsql.crt
-----BEGIN CERTIFICATE-----
<HERE>
-----END CERTIFICATE-----
EOF

update-ca-certificates

PEM_FILE="/etc/ssl/certs/cloudsql.pem"
PASSWORD="changeit"
JAVA_HOME=$(readlink -f /usr/bin/java | sed "s:bin/java::")
KEYSTORE="$JAVA_HOME/lib/security/cacerts"
CERTS=$(grep 'END CERTIFICATE' $PEM_FILE| wc -l)
echo $KEYSTORE
# To process multiple certs with keytool, you need to extract
# each one from the PEM file and import it into the Java KeyStore.
for N in $(seq 0 $(($CERTS - 1))); do
  ALIAS="$(basename $PEM_FILE)-$N"
  echo "Adding to keystore with alias:$ALIAS"
  cat $PEM_FILE |
    awk "n==$N { print }; /END CERTIFICATE/ { n++ }" |
    keytool -noprompt -import -trustcacerts \
            -alias $ALIAS -keystore $KEYSTORE -storepass $PASSWORD
done
#echo "export REQUESTS_CA_BUNDLE=/etc/ssl/certs/ca-certificates.crt" >> /databricks/spark/conf/spark-env.sh
#echo "export SSL_CERT_FILE=/etc/ssl/certs/ca-certificates.crt" >> /databricks/spark/conf/spark-env.sh