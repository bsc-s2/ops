#!/bin/sh


ip=${1-127.0.0.1}

cmd='curl -s "http://'$ip'/?extra&op=front-stat/distribution/client/:all/:all/total-time.json" | python -m json.tool | column -t'

watch -d -n5 "$cmd"
