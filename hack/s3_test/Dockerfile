FROM moby/buildkit AS buildkit

FROM debian:bullseye-slim

RUN apt-get update \
  && apt-get install -y --no-install-recommends wget ca-certificates containerd curl \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*

RUN mkdir /test

ARG TARGETOS TARGETARCH
RUN wget -q https://dl.min.io/server/minio/release/${TARGETOS}-${TARGETARCH}/minio \
  && wget -q https://dl.min.io/client/mc/release/${TARGETOS}-${TARGETARCH}/mc \
  && chmod +x minio mc \
  && mv minio mc /bin

COPY --from=buildkit /usr/bin/buildkitd /usr/bin/buildctl /bin

COPY . /test
