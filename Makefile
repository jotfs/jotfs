protos:
	protoc internal/protos/upload/upload.proto --twirp_out=. --go_out=.

run:
	go run ./cmd/server