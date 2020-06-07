protos:
	protoc internal/protos/api.proto --twirp_out=. --go_out=.

build:
	mkdir -p bin
	go build -ldflags="-s -w" -o ./bin/jotfs ./cmd/jotfs

tests:
	rm -f jotfs.db
	docker run --rm --name minio-jotfs-testing -p 9003:9000 -d minio/minio server /tmp/data
	go test -race -coverprofile=coverage.txt -covermode=atomic ./...
	docker stop minio-jotfs-testing
