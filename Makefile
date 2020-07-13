default: vet security test build

build:
	docker build --build-arg BUILD="${CIRCLE_SHA1}" -t retry-queue .

security:
	go get github.com/securego/gosec/cmd/gosec/...
	gosec -exclude=G104 ./...

ci:
	GOMAXPROCS=128 go test -v -race ./...

test:
	# go test -v -cover -race ./...
	GOMAXPROCS=128 CGO_ENABLED=0 go test -v ./...

vet:
	go vet ./...

protocol:
	flatc --go-namespace flatbuf --filename-suffix .gen --gen-onefile --go -o ./flatbuf protocol/requeue_msg.fbs

left:
	GOMAXPROCS=128 CGO_ENABLED=0 go run cmd/left/main.go

right:
	GOMAXPROCS=128 CGO_ENABLED=0 go run cmd/right/main.go

.PHONY: default build generate security test vet protocol left right ci