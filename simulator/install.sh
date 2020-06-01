#!/bin/bash

mkdir -p bin

wget https://dl.min.io/server/minio/release/linux-amd64/minio -O bin/minio
chmod +x bin/minio

go build -o ./bin/iotafs ../cmd/iotafs
