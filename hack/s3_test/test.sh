#!/bin/sh -e

/bin/minio server /tmp/data --console-address=0.0.0.0:9001 &
sleep 1
mc alias set myminio http://172.17.0.2:9000 minioadmin minioadmin
mc mb myminio/my-bucket
mc admin trace myminio &

buildkitd &

sleep 2

# First build: no cache on s3
# 4 files should be exported (2 blobs + 2 manifests)
buildctl build \
  --progress plain \
  --frontend dockerfile.v0 \
  --local context=/test/test1 \
  --local dockerfile=/test/test1 \
  --import-cache type=s3,bucket=my-bucket,name=foo,region=us-east-1,endpoint_url=http://127.0.0.1:9000,access_key_id=minioadmin,secret_access_key=minioadmin,s3_force_path_style=true \
  --export-cache "type=s3,mode=max,bucket=my-bucket,name=foo;bar,region=us-east-1,endpoint_url=http://127.0.0.1:9000,access_key_id=minioadmin,secret_access_key=minioadmin,s3_force_path_style=true"

# Check the 3 files are on s3
mc ls --recursive myminio/my-bucket | wc -l | grep 4

# Check we can reuse the cache
buildctl prune

buildctl build \
  --progress plain \
  --frontend dockerfile.v0 \
  --local context=/test/test2 \
  --local dockerfile=/test/test2 \
  --import-cache type=s3,bucket=my-bucket,name=bar,region=us-east-1,endpoint_url=http://127.0.0.1:9000,access_key_id=minioadmin,secret_access_key=minioadmin,s3_force_path_style=true \
  2>&1 | tee /tmp/log

# Check the first step was not executed, but read from S3 cache
cat /tmp/log | grep 'echo first' -A1 | grep CACHED

echo S3 Checks ok