# JotFS

[![Build Status](https://travis-ci.org/jotfs/jotfs.svg?branch=master)](https://travis-ci.org/jotfs/jotfs) [![codecov](https://codecov.io/gh/jotfs/jotfs/branch/master/graph/badge.svg)](https://codecov.io/gh/jotfs/jotfs) [![Go Report Card](https://goreportcard.com/badge/github.com/jotfs/jotfs)](https://goreportcard.com/report/github.com/jotfs/jotfs) [![Gitter](https://badges.gitter.im/jotfs/community.svg)](https://gitter.im/jotfs/community?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge)

JotFS is a deduplicating file storage engine backed by S3. It works by splitting incoming files into small content-defined chunks and only storing those chunks which it has not seen yet.

Features:

  - Reduces storage space
  - Reduces upload bandwidth
  - Backed by S3 or S3 compatible storage (Minio, Azure blob, GCP etc.)
  - [Client library](https://github.com/jotfs/jot) available for Go (more languages planned)
  - [Client CLI](https://github.com/jotfs/jot) with familiar commands: `jot cp`, `jot ls` etc.
  - Optional file versioning
  - Easy deployment with a single binary or Docker image

JotFS is currently beta level software.

## Install

### Docker
```
docker pull jotfs/jotfs
```

### Linux Binary

Download the latest binary from the [releases](https://github.com/jotfs/jotfs/releases) page and extract. Example:
```
gzip -dc ./jotfs_linux_amd64_v0.0.3.gz > jotfs
chmod +x ./jotfs
./jotfs -version
```

### Source

```
git clone https://github.com/jotfs/jotfs.git
cd jotfs
CGO_ENABLED=1 go build ./cmd/jotfs
```

## Quickstart

Using AWS S3 backed storage (see the [wiki](https://github.com/jotfs/jotfs/wiki) for more examples and advanced configuration):

```
jotfs -store_bucket="jotfs-test"
```

Use the [`jot`](https://github.com/jotfs/jot) CLI to interact with the server:
```
jot cp data.txt jot://data.txt

jot ls /

jot cp jot://data.txt data_download.txt
```

The server stores metadata in a database file located at `./jotfs.db` by default. When running the Docker image, you should mount a volume to `/app` in the container so the database is persisted between runs:
```
docker run -v jotfs:/app jotfs/jotfs <FLAGS...>
```

## License

JotFS is licensed under the Apache 2.0 License. See [LICENSE](./LICENSE) for details.

