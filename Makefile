default: vet security test build

build:
	docker build --build-arg BUILD="${CIRCLE_SHA1}" -t retry-queue .

security:
	go get github.com/securego/gosec/cmd/gosec/...
	gosec -exclude=G104 ./...

ci:
	# Running without race because CI times out
	# GOMAXPROCS=128 go test -race ./...
	GOMAXPROCS=128 go test ./...

test:
	# go test -v -cover -race ./...
	GOMAXPROCS=128 CGO_ENABLED=0 go test -cover ./...

vet:
	go vet ./...

protocol:
	flatc --gen-mutable --go-namespace flatbuf --filename-suffix .gen --gen-onefile --go -o ./flatbuf protocol/requeue_msg.fbs
	flatc --gen-mutable --go-namespace flatbuf --filename-suffix .gen --gen-onefile --go -o ./flatbuf protocol/stats_msg.fbs

left:
	GOMAXPROCS=128 CGO_ENABLED=0 go run cmd/left/main.go

right:
	GOMAXPROCS=128 CGO_ENABLED=0 go run cmd/right/main.go

.PHONY: default build generate security test vet protocol left right ci