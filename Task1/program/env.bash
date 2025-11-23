#!/bin/bash

export APP_PORT=5000
export LOGGER_HOST="192.168.122.4" 
export LOGGER_PORT=9999

declare -A NODE_IPS
NODE_IPS=(
    ["A"]="192.168.122.2"     
    ["B"]="192.168.122.3"     
    ["D"]="192.168.122.5"
)
