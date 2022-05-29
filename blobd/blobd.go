// Copyright 2020 Michael J. Fromberger. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Program blobd exports a blob.Store via JSON-RPC.
//
// By default, building or installing this tool includes a minimal set of
// storage backends, "file" and "memory". To build with additional storage
// support, add build tags for each, for example:
//
//   go install -tags badger,s3,gcs github.com/creachadair/ffstools/blobd@latest
//
// To include all available storage implementations, use the tag "all".
// The storage implementations currently supported include:
//
//   Tag      | Description
//   ---------|--------------------------------------------------------------------------
//   badger   | BadgerDB   https://godoc.org/github.com/dgraph-io/badger/v3
//   bitcask  | Bitcask    https://godoc.org/git.mills.io/prologic/bitcask
//   bolt     | BoltDB     https://godoc.org/go.etcd.io/bbolt
//   file     | Files      https://godoc.org/github.com/creachadair/ffs/storage/filestore
//   gcs      | GCS        https://godoc.org/cloud.google.com/go/storage
//   leveldb  | LevelDB    https://godoc.org/github.com/syndtr/goleveldb/leveldb
//   memory   | In-memory  https://godoc.org/github.com/creachadair/ffs/blob/memstore
//   pebble   | PebbleDB   https://godoc.org/github.com/cockroachdb/pebble
//   pogreb   | Pogreb     https://godoc.org/github.com/akrylysov/pogreb
//   s3       | Amazon S3  https://godoc.org/github.com/aws/aws-sdk-go/service/s3
//   sqlite   | SQLite     https://godoc.org/crawshaw.io/sqlite
//
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"runtime/debug"
	"sort"
	"strings"
	"syscall"

	"github.com/creachadair/ctrl"
	"github.com/creachadair/ffs/blob"
	"github.com/creachadair/ffs/blob/memstore"
	"github.com/creachadair/ffs/storage/filestore"
	"github.com/creachadair/ffstools/blobd/store"
)

var (
	listenAddr = flag.String("listen", "", "Service address (required)")
	storeAddr  = flag.String("store", "", "Store address (required)")
	keyFile    = flag.String("keyfile", "", "Encryption key file")
	bufferDB   = flag.String("buffer", "", "Write-behind buffer database")
	cacheSize  = flag.Int("cache", 0, "Memory cache size in MiB (0 means no cache)")
	doDebug    = flag.Bool("debug", false, "Enable server debug logging")
	zlibLevel  = flag.Int("zlib", 0, "Enable ZLIB compression (0 means no compression)")
	doVersion  = flag.Bool("version", false, "Print version information and exit")
	serveMode  = flag.String("mode", "jrpc2", "Service mode (jrpc2 or chirp)")

	// These storage implementations are built in by default.
	// To include other stores, build with -tags set to their names.
	// The known implementations are in the store_*.go files.
	stores = store.Registry{
		"file":   filestore.Opener,
		"memory": memstore.Opener,
	}
)

func init() {
	flag.Usage = func() {
		var keys []string
		for key := range stores {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		fmt.Fprintf(os.Stderr, `Usage: %[1]s [options] -store <spec> -listen <addr>

Start a server that serves content from the blob.Store described by the -store spec.
The server listens at the specified address, which may be a host:port or the path
of a Unix-domain socket.

A store spec is a storage type and address: type:address
The types understood are: %[2]s

If -listen is a host:port address, a TCP listener is created at that address.
Otherwise the address must be a path for a Unix-domain socket.
JSON-RPC data are exchanged with each message on one line, ending with newline.

With -keyfile, the store is opened with AES encryption.
Use -cache to enable a memory cache over the underlying store.

Options:
`, filepath.Base(os.Args[0]), strings.Join(keys, ", "))
		flag.PrintDefaults()
	}
}

func main() {
	flag.Parse()
	ctrl.Run(func() error {
		switch {
		case *doVersion:
			return printVersion()
		case *listenAddr == "":
			ctrl.Exitf(1, "You must provide a non-empty -listen address")
		case *storeAddr == "":
			ctrl.Exitf(1, "You must provide a non-empty -store address")
		}

		ctx := context.Background()
		bs, buf := mustOpenStore(ctx)
		defer func() {
			if err := blob.CloseStore(ctx, bs); err != nil {
				log.Printf("Warning: closing store: %v", err)
			}
		}()
		log.Printf("Store address: %q", *storeAddr)
		if *zlibLevel > 0 {
			log.Printf("Compression enabled: ZLIB level %d", *zlibLevel)
			if *keyFile != "" {
				log.Printf(">> WARNING: Compression and encryption are both enabled")
			}
		}
		if *cacheSize > 0 {
			log.Printf("Memory cache size: %d MiB", *cacheSize)
		}
		if *keyFile != "" {
			log.Printf("Encryption key: %q", *keyFile)
		}

		config := startConfig{
			Address: *listenAddr,
			Store:   bs,
			Buffer:  buf,
		}

		var closer closer
		var errc <-chan error
		switch *serveMode {
		case "jrpc", "jrpc2":
			closer, errc = startJSONServer(ctx, config)
		case "chirp":
			closer, errc = startChirpServer(ctx, config)
		default:
			ctrl.Fatalf("Unknown service -mode %q", *serveMode)
		}

		sig := make(chan os.Signal, 2)
		signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
		go func() {
			s, ok := <-sig
			if ok {
				log.Printf("Received signal: %v, closing listener", s)
				closer()
				signal.Reset(syscall.SIGINT, syscall.SIGTERM)
			}
		}()
		return <-errc
	})
}

func printVersion() error {
	bi, ok := debug.ReadBuildInfo()
	if !ok {
		return errors.New("no version information is available")
	}
	rev := "(unknown)"
	time := "(unknown)"
	for _, s := range bi.Settings {
		switch s.Key {
		case "vcs.revision":
			rev = s.Value
		case "vcs.time":
			time = s.Value
		}
	}
	fmt.Printf("%s built by %s at time %s rev %s\n",
		filepath.Base(os.Args[0]), bi.GoVersion, time, rev)
	return nil
}
