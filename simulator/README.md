# JotFS Simulator

The simulator runs end-to-end integration tests on the JotFS server. We use the `cmd/testdata` utility to generate test data and the latest version of the `jot` client CLI to interact with a running server.

The simulator is run during every build of our [Travis CI process](https://travis-ci.org/github/jotfs/jotfs).

## Running the simulator

The simulator requires Python3.6+ and Docker (to run [minio](https://github.com/minio/minio)). To run the simulator execute:

```
make run
```