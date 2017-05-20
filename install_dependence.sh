#!/bin/sh
yum install python-pip -y || { echo 'install pip fail'; exit 1; }

pip install pyyaml || { echo 'install pyyaml fail'; exit 1; }

pip install boto3 || { echo 'install boto3 fail'; exit 1; }
