FROM debian:bullseye-slim

RUN apt-get update \
  && apt-get install -y --no-install-recommends wget ca-certificates containerd

RUN mkdir /test

RUN wget -q https://dl.min.io/server/minio/release/linux-amd64/minio \
  && wget -q https://dl.min.io/client/mc/release/linux-amd64/mc \
  && chmod +x minio mc \
  && mv minio mc /bin

COPY hack/s3_test/ /test

COPY bin/buildkitd bin/buildctl /bin

RUN chmod +x /test/test.sh /bin/buildkitd
