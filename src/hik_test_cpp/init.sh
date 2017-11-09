#!/bin/sh

which make wget

if [ $? -ne 0 ];then
    yum -y install make wget
fi

if [ ! -d /usr/local/include/aws ];then
    if [ ! -f aws.tar.gz ];then
        wget -q  http://s2.i.qingcdn.com/s2-package/aws.tar.gz
    fi
    tar -zxvf aws.tar.gz  -C /usr/local/include/
fi

make
