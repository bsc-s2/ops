#!/bin/bash

chmod +x ./hik_tt

LD_PRELOAD=./libaws-cpp-sdk-core.so:./libaws-cpp-sdk-s3.so ./hik_tt
