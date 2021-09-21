#!/bin/sh

#
# Copyright (c) 2021 - for information on the respective copyright owner
# see the NOTICE file and/or the repository https://github.com/carbynestack/castor.
#
# SPDX-License-Identifier: Apache-2.0
#

# Print all executed commands to the terminal
#set -x

_init () {
    scheme="http://"
    address="$(netstat -nplt 2>/dev/null | awk ' /(.*\/java)/ { gsub(":::","127.0.0.1:",$4); print $4}')"
    resource="/actuator/health"
}

healthcheck_main () {
    # Get the http response code
    http_response=$(curl -H "User-Agent: Mozilla" -s -k -o /dev/null -I -w "%{http_code}" \
        ${scheme}${address}${resource})

    # Get the http response body
#    http_response_body=$(curl -H "User-Agent: Mozilla" -k -s ${scheme}${address}${resource})

    # server returns response 403 and body "SSL required" if non-TLS
    # connection is attempted on a TLS-configured server. Change
    # the scheme and try again
#    if [ "$http_response" = "403" ] && \
#    [ "$http_response_body" = "SSL required" ]; then
#        scheme="https://"
#        http_response=$(curl -H "User-Agent: Mozilla" -s -k -o /dev/null -I -w "%{http_code}" \
#            ${scheme}${address}${resource})
#    fi

    # If http_repsonse is 200 - server is up.
    [ "$http_response" = "200" ]
}

_init && healthcheck_main
