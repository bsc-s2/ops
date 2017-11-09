#!/bin/bash

if [ $# != 2 ];then
    echo '!!!!!!!! please input file_size(KB) and max_rps'
    echo 'like: sh start.sh 100 16000'
    exit
fi

file_size=$1
max_rps=$2

bucket='xxx'
access_key='xxx'
secret_key='xxx'
addr='xxx'

operator_type='upload'
thread_count='100'
test_time='3600'
file_name="${file_size}K.file"

echo "create test file"
dd if=/dev/zero of=${file_name} bs=1k count=${file_size}

echo "create config file"
echo "operator_type:${operator_type},thread_count:${thread_count},test_time:${test_time},file_name:${file_name},open_log:true,max_rps:${max_rps},bucket:${bucket},access_key:${access_key},secret_key:${secret_key}"  > config

echo "start test, you can run sh stop.sh top suspend test"
nohup sh ./run.sh &

dstat
