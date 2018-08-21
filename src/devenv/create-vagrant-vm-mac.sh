#!/bin/bash

die()
{
    echo "$@"
    exit 1
}

boxname=centos-7-2
boxfn=centos-7-2-virtualbox.box


if which vagrant >/dev/null 2>/dev/null; then
    :
else
    # Install vagrant
    brew install virtualbox || die install virtualbox
    brew cask install vagrant || die install vagrant
fi

if vagrant box list | grep -q "^$boxname"; then
    :
else
    # Download and add the virtualbox image for centos-7.

    curl "http://bspackage.ss.bscstorage.com/vagrant-box/$boxfn?AWSAccessKeyId=yqs7ofbweuzk8p59ca6n&Expires=1621133661&Signature=QLnjatuLZJVA5oX95litgSmdw9E%3D" \
        > $boxfn \
        || die download image

    vagrant box add --name $boxname $boxfn \
        || die vagrant box add

    rm $boxfn
fi

# Create VM config:

vagrant init $boxname || die init $boxname

vagrant up
