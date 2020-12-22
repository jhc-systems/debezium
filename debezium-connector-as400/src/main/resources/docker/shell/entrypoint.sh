#!/bin/bash

MYIP=$(awk 'END{print $1}' /etc/hosts)

echo "My ip address is $MYIP"

# watch the replacement seperator for URLs
sed -e "s#SCHEMA_URL#$SCHEMA_URL#" -e "s/GROUP_ID/$GROUP_ID/" \
	-e "s/BOOTSTRAP_SERVERS/$BOOTSTRAP_SERVERS/" -e "s/REST_HOST_NAME/$REST_HOST_NAME/" \
	connect-distributed-template.properties > connect-distributed.properties

cat connect-distributed.properties

jars=$(echo *jar|tr ' ' ':')
java -cp $jars -javaagent:/app/jmx_prometheus_javaagent-0.12.0.jar=7071:/app/debezium-jmx-pometheus.yml -Dlogback.configurationFile=logback.xml org.apache.kafka.connect.cli.ConnectDistributed connect-distributed.properties
