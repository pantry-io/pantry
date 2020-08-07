# nats-requeue

[![GoDoc](https://godoc.org/github.com/nickpoorman/nats-requeue?status.svg)](https://godoc.org/github.com/nickpoorman/nats-requeue)
[![CircleCI](https://circleci.com/gh/nickpoorman/nats-requeue.svg?style=svg)](https://circleci.com/gh/nickpoorman/nats-requeue)
[![License](https://img.shields.io/badge/license-Apache%202-blue)](LICENSE.txt)

This is a service that persists [NATS](https://github.com/nats-io) messages to
disk and retries sending them at a deferred time.

# THIS IS STILL BEING IMPLEMENTED. IT IS NOT READY FOR USE.

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

I would recommend setting your soft ulimit to `65535`. Google how to do it on your
[OS](https://gist.github.com/luckydev/b2a6ebe793aeacf50ff15331fb3b519d).

### AWS ECS

## Uses

### DLQ (dead letter queue)

TODO

### Disk-backed Buffer

TODO

## How Requeue Works

All queue meta information is kept in memory and synced to disk.

**Checkpointing**

In memory meta for a queue consists of a Checkpoint. The checkpoint is used for
seeking to the a key in Badger. This allows us to iterate directly between the
events we are looking for in a time range.

## Contributing

This project uses [Flatbuffers](https://github.com/google/flatbuffers) to
serialize messages. To build the `protocol/*.fbs` files you must have `flatc`
installed and then run the following.

```
make protocol
```

## Thanks

- [NATS](https://docs.nats.io/) for an awesome distributed messaging system.
- This project uses [dgraph-io/badger](https://github.com/dgraph-io/badger) for
  storing the messages on disk.
