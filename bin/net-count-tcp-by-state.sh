#!/bin/sh

netstat -tan \
    | grep tcp \
    | awk '
{
    s[$NF]+=1
}
END {
    for (i in s) {
        printf ("%10s %d\n",i,s[i])
    }
}'
