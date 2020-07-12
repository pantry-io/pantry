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

protoc:
	docker pull namely/protoc-all
	docker run -v `pwd`:/protocol namely/protoc-all -f requeue_msg.fbs -l go

.PHONY: default build generate security test vet