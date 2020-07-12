default: vet security test build

build:
	docker build --build-arg BUILD="${CIRCLE_SHA1}" -t retry-queue .

security:
	go get github.com/securego/gosec/cmd/gosec/...
	gosec -exclude=G104 ./...

test:
	go test -v -cover -race ./...

vet:
	go vet ./...

protocol:
	flatc --go-namespace requeue --filename-suffix .gen --gen-onefile --go -o . protocol/requeue_msg.fbs

.PHONY: default build generate security test vet protocol