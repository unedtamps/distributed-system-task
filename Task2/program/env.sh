#!/bin/bash

export LOGGER_HOST="192.168.122.5"
export LOGGER_PORT=9000

export NUMNODES=3
export USE_MUTEX=1
export TCP_PORT=8000
export UDP_PORT=8100

declare -A NODE_IP=(
  [1]="192.168.122.2"
  [2]="192.168.122.3"
  [3]="192.168.122.4"
)
