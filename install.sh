#!/bin/sh

ln -sfT $PWD/src/upload_directory.py bin/upload_directory.py || { echo 'Failed linking upload_directory.py'; exit 1; }
ln -sfT $PWD/src/s2http_benchmark.py bin/s2http_benchmark.py || { echo 'Failed linking s2http_benchmark.py'; exit 1; }
