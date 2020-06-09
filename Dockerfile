FROM golang:1.14-alpine3.12 as builder

ENV GO111MODULE=on \
    CGO_ENABLED=1

WORKDIR /build

COPY . .

RUN apk add --update gcc musl-dev && \
    go build -ldflags="-s -w" ./cmd/jotfs

FROM alpine:3

WORKDIR /app

COPY --from=builder /build/jotfs .

ENTRYPOINT ["./jotfs"]
