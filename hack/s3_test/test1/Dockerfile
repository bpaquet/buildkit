FROM public.ecr.aws/debian/debian:bullseye-slim

RUN cat /dev/urandom | head -c 100 | sha256sum > unique_first
RUN cat /dev/urandom | head -c 100 | sha256sum > unique_second