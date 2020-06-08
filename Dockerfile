FROM golang:1.14-buster as builder

ENV GO111MODULE=on \
    CGO_ENABLED=1

WORKDIR /build

COPY . .

RUN go build -ldflags="-s -w" ./cmd/jotfs

FROM debian:buster-slim

COPY --from=builder /build/jotfs .

ENTRYPOINT ["./jotfs"]