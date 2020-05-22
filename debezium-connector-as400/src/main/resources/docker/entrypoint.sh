#!/bin/bash

MYIP=$(awk 'END{print $1}' /etc/hosts)

sed "s/MYIP/$MYIP/" connect-distributed.template > connect-distributed.properties

jars=$(echo *jar|tr ' ' ':')
java -cp $jars -Dlogback.configurationFile=logback.xml org.apache.kafka.connect.cli.ConnectDistributed connect-distributed.properties
