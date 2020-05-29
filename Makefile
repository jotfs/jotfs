protos:
	protoc internal/protos/api.proto --twirp_out=. --go_out=.

run:
	go run ./cmd/iotafs