#!/bin/env bash

pids=$(ps aux | grep hik_tt | grep -v grep | awk '{print $2}')

if [  -n "$pids" ];then
    for p in $pids; do kill $p; done
else
    echo "Nosuch process hik_tt"
fi
