# Command-line tools for FFS

See also https://github.com/creachadair/ffs.

[![CI](https://github.com/creachadair/ffstools/actions/workflows/go-presubmit.yml/badge.svg?event=push&branch=main)](https://github.com/creachadair/ffstools/actions/workflows/go-presubmit.yml)

## Overview

- The [`ffs`](https://github.com/creachadair/ffstools/tree/main/ffs) tool
  supports running and communicating with a blob storage service, and provides
  commands to manipulate the contents of the store as FFS specific messages.

  ```sh
  # To install:
  go install github.com/creachadair/ffstools/ffs@latest
  ```

  The `ffs blob` subcommand replaced the formerly separate `blob` tool ([#30][]).
  The `ffs storage` subcommand replaced the formerly separate `blobd` tool ([#35][]).

  When installing, you may want to specify build `--tags` to enable other
  storage backends. See [Storage Backends](#storage-backends).

[#30]: https://github.com/creachadair/ffstools/pull/30
[#35]: https://github.com/creachadair/ffstools/pull/35

- The [`file2json`](https://github.com/creachadair/ffstools/tree/main/file2json)
  tool decodes wire-format node messages and translates them to JSON for easier
  reading by humans.

  ```sh
  # To install:
  go install github.com/creachadair/ffstools/file2json@latest
  ```

## Installation and Usage

Install `ffs` as noted above, then:

```bash
# Start up a storage server using local files as storage.
export FFS_STORE=/tmp/test.db.sock
ffs storage -store file:test.db -listen "$FFS_STORE" &
while [[ ! -e "$FFS_STORE" ]] ; do sleep 1 ; done

# Create a root pointer to anchor some data.
ffs root create example --desc 'Example root pointer'

# Create some files to put into storage.
mkdir -p files/sub
echo "This is my file." > files/sub/f1.txt
echo "Many others are like it" > files/sub/f2.txt
echo "But this one is mine." > files/f3.txt

# Copy the files directory into the store.
ffs put -into example/test1 files

# List the contents we just wrote...
ffs file list -long example/test1
ffs file list -long example/test1/sub

# Move some files around...
echo "That was your file." > files/sub/f1.txt
mv files/sub/f2.txt files/f4.txt
rm files/f3.txt

# Add another copy of the structure.
ffs put -into example/test2 files

# List the revised contents...
ffs file list -long example/test1
ffs file list -long example/test2
ffs file list -long example/test2/sub

# List the stuff reachable from the root.
ffs file list -long -key example

# GC unreachable data in the store.
ffs gc

# Stop the storage server.
kill %1 && wait
```

## Storage Backends

The following storage backends are currently supported by default:

| Type   | Description                  | Implementation                                                 |
|--------|------------------------------|----------------------------------------------------------------|
| memory | In-memory storage (built-in) | https://godoc.org/github.com/creachadair/ffs/blob/memstore     |
| file   | Local directory (built-in)   | https://godoc.org/github.com/creachadair/ffs/storage/filestore |

The following storage backends can be enabled by building with the specified tags:

| Type/Tag | Description | Implementation                                            |
|----------|-------------|-----------------------------------------------------------|
| badger   | BadgerDB    | https://godoc.org/github.com/dgraph-io/badger/v4          |
| bitcask  | Bitcask     | https://godoc.org/git.mills.io/prologic/bitcask           |
| bolt     | BoltDB      | https://godoc.org/go.etcd.io/bbolt                        |
| gcs      | GCS         | https://godoc.org/cloud.google.com/go/storage             |
| leveldb  | LevelDB     | https://godoc.org/github.com/syndtr/goleveldb/leveldb     |
| pebble   | PebbleDB    | https://godoc.org/github.com/cockroachdb/pebble           |
| pogreb   | Pogreb      | https://godoc.org/github.com/akrylysov/pogreb             |
| s3       | Amazon S3   | https://godoc.org/github.com/aws/aws-sdk-go-v2/service/s3 |
| sqlite   | SQLite3     | https://godoc.org/modernc.org/sqlite                      |
