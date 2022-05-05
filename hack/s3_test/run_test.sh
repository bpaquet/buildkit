#!/bin/sh -e

mv .dockerignore .dockerignore.s3_test
function finish {
  mv .dockerignore.s3_test .dockerignore
}
trap finish EXIT

docker build -f hack/s3_test/Dockerfile . -t s3_test
docker run --rm -ti --privileged s3_test /test/test.sh