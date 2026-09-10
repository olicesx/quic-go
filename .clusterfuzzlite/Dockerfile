FROM gcr.io/oss-fuzz-base/base-builder-go:v1

ARG TARGETPLATFORM
RUN echo "TARGETPLATFORM: ${TARGETPLATFORM}"

RUN apt-get update && apt-get install -y make autoconf automake libtool

COPY . $SRC/quic-go
WORKDIR quic-go
COPY .clusterfuzzlite/build.sh $SRC/
