#!/bin/sh

# tail aws_json.access.log and split it into fields

keyword="${1-.}"

tail -F aws_json.access.log \
    | grep -v "$keyword" \
    | grep -o "[^ ]*"
