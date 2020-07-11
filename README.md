# nats-requeue

[![GoDoc](https://godoc.org/github.com/nickpoorman/nats-requeue?status.svg)](https://godoc.org/github.com/nickpoorman/nats-requeue)
[![CircleCI](https://circleci.com/gh/nickpoorman/nats-requeue.svg?style=svg)](https://circleci.com/gh/nickpoorman/nats-requeue)
[![License](https://img.shields.io/badge/license-Apache%202-blue)](LICENSE.txt)

This is a service that persists [NATS](https://github.com/nats-io)
messages to disk and retries sending them at a deferred time.

## Getting Started

### Installing

```
go get github.com/nickpoorman/nats-requeue
```

This will retrieve the library and install the retry-queue command line utility
into your `$GOBIN` path.

### Configuration

Badger
[recommends](https://github.com/dgraph-io/badger#are-there-any-go-specific-settings-that-i-should-use)
running with `GOMAXPROCS=128`.

### AWS ECS

## Thanks

This project uses [dgraph-io/badger](https://github.com/dgraph-io/badger) under
the hood to store the messages.
