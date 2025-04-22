#!/bin/bash

curl -sku bret:2xqT2IO1OQ%tfMHP -X DELETE "https://192.168.10.31:9200/deepfreeze-status"
curl -sku bret:2xqT2IO1OQ%tfMHP -X DELETE "https://192.168.10.31:9200/_data_stream/deepfreeze-testing"

aws s3 rm s3://bdw-eah-test --recursive
aws s3api delete-bucket --bucket your-bucket-name