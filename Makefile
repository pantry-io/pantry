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

.PHONY: default build generate security test vet