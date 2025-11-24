#!/bin/bash

export LOGGER_HOST="localhost"
export LOGGER_PORT=9000

export NUMNODES=3
export USE_MUTEX=1
export TCP_PORT=8000
export UDP_PORT=8100

declare -A NODE_IP=(
  [1]="localhost"
  [2]="localhost"
  [3]="localhost"
)
