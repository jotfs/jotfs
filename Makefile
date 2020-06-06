protos:
	protoc internal/protos/api.proto --twirp_out=. --go_out=.

run: protos
	go run ./cmd/jotfs

tests:
	docker run --rm --name minio-jotfs-testing -p 9003:9000 -d minio/minio server /tmp/data
	go test -race -coverprofile=coverage.txt -covermode=atomic ./...
	docker stop minio-jotfs-testing
