#!/bin/bash

source ../program/env.bash
NODE="B"
PEERS_STR=""
for name in "${!NODE_IPS[@]}"; do
    ip="${NODE_IPS[$name]}"
    PEERS_STR+="$name@$ip:$APP_PORT "
done


python3 peer_node.py \
	  --name "$NODE" --listen 0.0.0.0 "$APP_PORT" \
	  --peers $PEERS_STR \
	  --logger "$LOGGER_HOST" 9999 \
	  --offset-ms -600 \

