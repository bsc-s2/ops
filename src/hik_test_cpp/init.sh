#!/bin/sh

wich make wget

if [ $? -ne 0 ];then
    yum -y install make wget
fi

wget -q  http://s2.i.qingcdn.com/s2-package/aws.tar.gz

if [ $? -eq 0 ];then
    tar -zxvf aws.tar.gz  -C /usr/local/include/
fi

make
