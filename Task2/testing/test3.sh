#!/bin/bash
#
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
python3 ./kvclient.py --nodes $NODES cmd --node 1  "PUT color black"
python3 ./kvclient.py --nodes $NODES cmd --node 2  "PUT color magenta"
python3 ./kvclient.py --nodes $NODES cmd --node 2  "PUT warna kuning"
python3 ./kvclient.py --nodes $NODES cmd --node 2  "PUT warna kuning"
python3 ./kvclient.py --nodes $NODES race "PUT color blue" "PUT color green" 
python3 ./kvclient.py --nodes $NODES race "PUT warna oranye" "PUT warna hijau" 


echo "-------HASIL----"
python3 ./kvclient.py --nodes $NODES  getall color
echo "-------HASIL----"
python3 ./kvclient.py --nodes $NODES  getall warna




