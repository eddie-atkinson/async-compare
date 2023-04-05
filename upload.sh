#!/bin/bash
BUCKET_NAME=testing-async
FILE_KEY=revised.parquet

aws s3 cp ./revised.parquet s3://$BUCKET_NAME/$FILE_KEY