#!/bin/bash
## curl-basic-auth
## - Sample of How to Create a Database and Catalog it in the Db2 Console
##   curl in bash
##   use jq to parse JSON, see https://stedolan.github.io/jq/download/
## version 0.0.1
##################################################

## Database to Create and Catalog
DATABASE=$1
CONNECTION=$2
CONNPORT=$3
CONNHOST=$4
CONNUSERID=$5
CONNPW=$6
CONNCOMMENT=$7

echo 'Create Database:' $DATABASE
db2 create database $DATABASE
echo

## Db2 Console Connection Information
HOST='http://localhost:11080'
USERID='db2inst1'
PASSWORD='db2inst1'

echo 'Connect to Console:' $HOST
TOKEN=$(curl -s -X POST $HOST/dbapi/v4/auth/tokens \
  -H 'content-type: application/json' \
  -d '{"userid": '$USERID' ,"password":'$PASSWORD'}' | jq -r '.token')
echo

echo 'Add Connection Profile:' $CONNECTION 'using the JSON payload:'

JSONPAYLOAD='{"name":"'$CONNECTION'","dataServerType":"DB2LUW","databaseName":"'$DATABASE'","port":"'$CONNPORT'","host":"'$CONNHOST'","URL":"jdbc:db2://'$CONNHOST':'$CONNPORT'/'$DATABASE':retrieveMessagesFromServerOnGetMessage=true;","sslConnection":"false","disableDataCollection":"false","collectionCred":{"securityMechanism":"3","user":"'$CONNUSERID'","password":"'$CONNPW'"},"operationCred":{"securityMechanism":"3","user":"'$CONNUSERID'","password":"'$CONNPW'","saveOperationCred":"true"},"comment":"'$CONNCOMMENT'"}'
echo $JSONPAYLOAD
echo

JSONRESULT=$(curl -s -X POST $HOST/dbapi/v4/dbprofiles \
  -H 'authorization: Bearer '$TOKEN \
  -H 'content-type: application/json' \
  -d $JSONPAYLOAD)
echo $JSONRESULT 
  


