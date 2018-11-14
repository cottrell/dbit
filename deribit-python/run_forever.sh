#!/usr/bin/env sh
DIR="$( cd "$(dirname "$0")" ; pwd -P )"
while true; do
    $DIR/do.py
    sleep 1
done
