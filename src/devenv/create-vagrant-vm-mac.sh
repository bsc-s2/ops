#!/bin/bash

die()
{
    echo "$@"
    exit 1
}

if which vagrant >/dev/null 2>/dev/null; then
    :
else
    # Install vagrant
    brew cask install vagrant || die install vagrant
fi

if vagrant box list | grep -q "^centos-7-14"; then
    :
else
    # Download and add the virtualbox image for centos-7.

    wget "http://bspackage.ss.bscstorage.com/vagrant-box/centos-7-14-1804-x86-64-virtualbox.box?AWSAccessKeyId=yqs7ofbweuzk8p59ca6n&Expires=1621021030&Signature=MSLB2AjoYOUKvG0zmgkEshzElKU%3D" \
        -O centos-7-14-1804-x86-64-virtualbox.box \
        || die download image

    vagrant box add --name centos-7-14 centos-7-14-1804-x86-64-virtualbox.box \
        || die vagrant box add

    rm centos-7-14-1804-x86-64-virtualbox.box
fi

# Create VM config:

vagrant init centos-7-14 || die init centos-7-14

vagrant up
