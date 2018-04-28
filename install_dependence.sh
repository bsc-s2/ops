#!/bin/sh
yum install python-pip -y || { echo 'install pip fail'; exit 1; }

pip install pyyaml || { echo 'install pyyaml fail'; exit 1; }

pip install boto3 || { echo 'install boto3 fail'; exit 1; }

pip install python-magic || { echo 'install python-magic fail'; exit 1; }

git clone https://github.com/bsc-s2/pykit.git src/pykit
