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

// Program blobd exports a blob.Store via the Chirp V0 protocol.
//
// By default, building or installing this tool includes a minimal set of
// storage backends, "file" and "memory". To build with additional storage
// support, add build tags for each, for example:
//
//	go install -tags badger,s3,gcs github.com/creachadair/ffstools/blobd@latest
//
// To include all available storage implementations, use the tag "all".
// The storage implementations currently supported include:
//
//	Tag      | Description
//	---------|--------------------------------------------------------------------------
//	badger   | BadgerDB   https://godoc.org/github.com/dgraph-io/badger/v3
//	bitcask  | Bitcask    https://godoc.org/git.mills.io/prologic/bitcask
//	bolt     | BoltDB     https://godoc.org/go.etcd.io/bbolt
//	file     | Files      https://godoc.org/github.com/creachadair/ffs/storage/filestore
//	gcs      | GCS        https://godoc.org/cloud.google.com/go/storage
//	leveldb  | LevelDB    https://godoc.org/github.com/syndtr/goleveldb/leveldb
//	memory   | In-memory  https://godoc.org/github.com/creachadair/ffs/blob/memstore
//	nuts     | NutsDB     https://godoc.org/github.com/xujiajun/nutsdb
//	pebble   | PebbleDB   https://godoc.org/github.com/cockroachdb/pebble
//	pogreb   | Pogreb     https://godoc.org/github.com/akrylysov/pogreb
//	s3       | Amazon S3  https://godoc.org/github.com/aws/aws-sdk-go/service/s3
//	sqlite   | SQLite     https://godoc.org/crawshaw.io/sqlite
//
// For information about Chirp v0, see:
// https://github.com/creachadair/chirp/blob/main/spec.md
package main

import (
	"archive/zip"
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"sort"
	"strings"
	"syscall"
	"time"

	"github.com/creachadair/command"
	"github.com/creachadair/ctrl"
	"github.com/creachadair/ffs/blob"
	"github.com/creachadair/ffs/blob/memstore"
	"github.com/creachadair/ffs/storage/filestore"
	"github.com/creachadair/ffs/storage/zipstore"
	"github.com/creachadair/ffstools/blobd/store"
)

var (
	// Flags (see root.SetFlags below).
	listenAddr string
	storeAddr  string
	keyFile    string
	aeadStyle  string
	doSignKeys bool
	bufferDB   string
	cacheSize  int
	zlibLevel  int
	doReadOnly bool
	doVersion  bool

	// These storage implementations are built in by default.
	// To include other stores, build with -tags set to their names.
	// The known implementations are in the store_*.go files.
	stores = store.Registry{
		"file": func(ctx context.Context, addr string) (blob.Store, error) {
			if strings.HasSuffix(addr, ".zip") {
				zf, err := zip.OpenReader(addr)
				if err != nil {
					return nil, err
				}
				return zipstore.New(zf, nil), nil
			}
			return filestore.Opener(ctx, addr)
		},
		"memory": memstore.Opener,
	}
)

func main() {
	// List the available storage drivers. Note we have to do this after main
	// starts to ensure all the initializers are done with registration.
	var storeNames []string
	for key := range stores {
		storeNames = append(storeNames, key)
	}
	sort.Strings(storeNames)

	root := &command.C{
		Name:  filepath.Base(os.Args[0]),
		Usage: `[options] -store <spec> -listen <addr>`,
		Help: fmt.Sprintf(`
Start a server that serves content from the blob.Store described by the -store spec.
The server listens at the specified address, which may be a host:port or the path
of a Unix-domain socket.

A store spec is a storage type and address: type:address
The types understood are: %[1]s

If -listen is:

 - A host:port address, a TCP listener is created at that address.`+tsAddress+`

 - Otherwise, the address must be a path for a Unix-domain socket.

With -keyfile, the store is opened with AES encryption.
If BLOBD_KEYFILE_PASSPHRASE is set in the environment, it is used as the
passphrase for the keyfile; otherwise blobd prompts at the terminal.

Use -cache to enable a memory cache over the underlying store.`, strings.Join(storeNames, ", ")),

		SetFlags: func(_ *command.Env, fs *flag.FlagSet) {
			fs.StringVar(&listenAddr, "listen", "", "Service address (required)")
			fs.StringVar(&storeAddr, "store", "", "Store address (required)")
			fs.StringVar(&keyFile, "keyfile", "", "Encryption key file (if empty, do not encrypt)")
			fs.StringVar(&aeadStyle, "encryption", "gcm", `Encryption algorithm ("gcm" or "chacha")`)
			fs.BoolVar(&doSignKeys, "sign-keys", false, "Sign content addresses (ignored without -keyfile)")
			fs.StringVar(&bufferDB, "buffer", "", "Write-behind buffer database")
			fs.IntVar(&cacheSize, "cache", 0, "Memory cache size in MiB (0 means no cache)")
			fs.IntVar(&zlibLevel, "zlib", 0, "Enable ZLIB compression (0 means no compression)")
			fs.BoolVar(&doReadOnly, "read-only", false, "Disallow modification of the store")
			fs.BoolVar(&doVersion, "version", false, "Print version information and exit")
		},

		Run: blobd,
	}
	ctrl.Run(func() error {
		command.RunOrFail(root.NewEnv(nil), os.Args[1:])
		return nil
	})
}

func printVersion() error {
	bi := getBuildInfo()
	if bi == nil {
		return errors.New("no version information is available")
	}
	if bi.modified {
		bi.revision += " (modified)"
	}
	fmt.Printf("%s built by %s at time %s rev %s\n",
		filepath.Base(os.Args[0]), bi.toolchain, bi.buildTime, bi.revision)
	return nil
}

func blobd(env *command.Env) error {
	switch {
	case doVersion:
		return printVersion()
	case listenAddr == "":
		ctrl.Exitf(1, "You must provide a non-empty -listen address")
	case storeAddr == "":
		ctrl.Exitf(1, "You must provide a non-empty -store address")
	}

	ctx := context.Background()
	bs, buf := mustOpenStore(ctx)
	defer func() {
		// N.B. Invoke close with a fresh context, since the parent is likely to
		// have been already canceled during shutdown. Set a timeout in case the
		// close gets stuck, however.
		cctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := bs.Close(cctx); err != nil {
			log.Printf("Warning: closing store: %v", err)
		}
	}()
	log.Printf("Store address: %q", storeAddr)
	if doReadOnly {
		log.Print("Store is open in read-only mode")
	}
	if zlibLevel > 0 {
		log.Printf("Compression enabled: ZLIB level %d", zlibLevel)
		if keyFile != "" {
			log.Printf(">> WARNING: Compression and encryption are both enabled")
		}
	}
	if cacheSize > 0 {
		log.Printf("Memory cache size: %d MiB", cacheSize)
	}
	if keyFile != "" {
		log.Printf("Encryption key: %q", keyFile)
	}

	config := startConfig{
		Address: listenAddr,
		Store:   bs,
		Buffer:  buf,
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	closer, loop := startChirpServer(ctx, config)
	sig := make(chan os.Signal, 2)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		s, ok := <-sig
		if ok {
			log.Printf("Received signal: %v, closing listener", s)
			cancel()
			closer()
			signal.Reset(syscall.SIGINT, syscall.SIGTERM)
		}
	}()
	return loop.Wait()
}
