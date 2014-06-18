#!/bin/bash

SCRIPTDIR=$( pwd )/$( dirname "${BASH_SOURCE-$0}" )
SCHEMA_DIR=$SCRIPTDIR/../test/fixtures/schemas
CURL=/usr/bin/curl
TASR_URL=http://dhdp2jump01:8080/tasr/topic
SCHEMAS=(browse_click_tracking gold login_detail message newsfeed_clicks page_view)

cd $SCHEMA_DIR;

for schema in ${SCHEMAS[*]}; do
    if [ -f $schema.avsc ]; then
        $CURL -X PUT -d @$schema.avsc -H "Content-Type: application/json" $TASR_URL/$schema
        echo "registered $schema.avsc for topic $schema"
        $CURL -X PUT -d @$schema.avsc -H "Content-Type: application/json" $TASR_URL/s_$schema
        echo "registered $schema.avsc for topic s_$schema"
    fi;
done; 