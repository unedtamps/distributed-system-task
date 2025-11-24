#!/bin/bash
#

source ../program/env.sh

NODES=""
first=1
for id in "${!NODE_IP[@]}"; do
    ip="${NODE_IP[$id]}"

    if [[ $first -eq 1 ]]; then
        NODES="${ip}:${TCP_PORT}"
        first=0
    else
        NODES="$NODES,${ip}:${TCP_PORT}"
    fi
done

# Single PUT to node 1
python3 ./kvclient.py --nodes $NODES cmd --node 1  "PUT color red"
python3 ./kvclient.py --nodes $NODES cmd --node 2  "PUT color blue"


# GET from node 2
python3 ./kvclient.py  --nodes $NODES cmd --node 2   "GET color"
python3 ./kvclient.py  --nodes $NODES cmd --node 0   "GET color"




