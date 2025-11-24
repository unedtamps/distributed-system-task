#!/bin/bash

source ./../program/env.sh
NODE=3
PEERS=""
first=1
for name in "${!NODE_IP[@]}"; do
    if [[ "$name" != "$NODE" ]]; then
        peer_ip="${NODE_IP[$name]}"

        if [[ $first -eq 1 ]]; then
            PEERS="${peer_ip}:${TCP_PORT}=${name}"
            first=0
        else
            PEERS="$PEERS,${peer_ip}:${TCP_PORT}=${name}"
        fi
    fi
done


python3 ./kv.py \
    --id $NODE \
    --tcp "$TCP_PORT" \
    --udp "$UDP_PORT" \
    --peers $PEERS \
    --logger-addr "$LOGGER_HOST:$LOGGER_PORT" \
    --numnodes "$NUMNODES" \
    --use-mutex "$USE_MUTEX"
