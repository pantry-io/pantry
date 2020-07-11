FROM golang:1.14

ARG BUILD=docker

WORKDIR /go/src/retry-queue
COPY . .

RUN go get -d -v ./...
RUN go install -mod vendor -ldflags "-X main.build=${BUILD}" -v ./...

CMD ["retry-queue"]