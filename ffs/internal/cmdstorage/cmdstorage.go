// Copyright 2021 Michael J. Fromberger. All Rights Reserved.
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

// Package cmdstorage implements the "ffs storage" subcommand.
package cmdstorage

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"log"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/creachadair/atomicfile"
	"github.com/creachadair/command"
	"github.com/creachadair/ffstools/ffs/config"
	"github.com/creachadair/ffstools/ffs/internal/cmdstorage/registry"
	"github.com/creachadair/ffstools/lib/storeservice"
	"github.com/creachadair/flax"
	"github.com/creachadair/getpass"
	"github.com/creachadair/keyfile"
)

var flags struct {
	ListenAddr string `flag:"listen,Service address (required)"`
	KeyFile    string `flag:"key,Encryption key file or salt (if empty, do not encrypt)"`
	BufferDB   string `flag:"buffer,Write-behind buffer database"`
	CacheSize  int    `flag:"cache,Memory cache size in MiB (0 means no cache)"`
	Compress   bool   `flag:"compress,Enable zstd compression of blob data"`
	ReadOnly   bool   `flag:"read-only,Disallow modification of the store"`
}

var Command = &command.C{
	Name:  "storage",
	Usage: "--store <spec> --listen <addr> [options]",
	Help: fmt.Sprintf(`Run a storage server.

Start a server that serves content from the blob.Store described by the
--store spec. The server listens at the --listen address, which may be
a host:port or the path of a Unix-domain socket.

A store spec is a storage type and address: type:address
The types understood are: %[1]s

If --store has the form "@name", the storage spec associated with that
name in the FFS config file is used (if defined).

The --listen flag must be one of:

 - A label of the form "@name": The address associated with that name
   in the FFS config file is used. If --listen is empty and the --store
   flag has the form "@name", it uses the address from that setting.

 - A host:port address: A TCP listener is created at that address.

 - Otherwise: The path for a Unix-domain socket.

With --cache, the server provides a memory cache over the primary store.

With --key, the store is opened with chacha20-poly1305 encryption.
The contents of the --key file are used as the cipher key.
If the file has the format of http://godoc.org/github.com/creachadair/keyfile,
it is unlocked using a passphrase, from the FFS_PASSPHRASE environment or
prompted at the terminal. Otherwise its contents are used verbatim

Use --buffer to enable a local write-behind buffer. The syntax of its
argument is the same as for --store. This is suitable for primary stores
that are remote and slow (e.g., cloud storage).`,
		strings.Join(registry.Stores.Names(), ", ")),

	SetFlags: command.Flags(flax.MustBind, &flags),
	Run:      command.Adapt(runStorage),

	Commands: []*command.C{{
		Name:  "keygen",
		Usage: "<key-file>",
		Help:  "Generate a random encryption key into the specified file.",
		Run:   command.Adapt(runKeyGen),
	}},
}

func runStorage(env *command.Env) error {
	s := env.Config.(*config.Settings)
	rs := s.ResolveSpec(s.DefaultStore)
	if rs.Spec == "" {
		return env.Usagef("you must provide a --store spec")
	}
	listenAddr, err := getListenAddr(env)
	if err != nil {
		return err
	}
	encryptionKey, err := getEncryptionKey(flags.KeyFile)
	if err != nil {
		return err
	}
	bs, buf, err := openStore(env.Context(), rs.Spec)
	if err != nil {
		return err
	}
	defer func() {
		// N.B. Invoke close with a fresh context, since the parent is likely to
		// have been already canceled during shutdown. Set a timeout in case the
		// close gets stuck, however.
		cctx, cancel := context.WithTimeout(env.Context(), 5*time.Second)
		defer cancel()
		if err := bs.Close(cctx); err != nil {
			log.Printf("Warning: closing store: %v", err)
		}
	}()
	log.Printf("Store: %q", rs.Spec)
	if flags.ReadOnly {
		log.Print("Store is open in read-only mode")
	}
	if flags.Compress {
		log.Print("Compression enabled (zstd)")
		if flags.KeyFile != "" {
			log.Printf(">> WARNING: Compression and encryption are both enabled")
		}
	}
	if flags.CacheSize > 0 {
		log.Printf("Memory cache size: %d MiB", flags.CacheSize)
	}
	if flags.KeyFile != "" {
		log.Printf("Encryption key: %q", flags.KeyFile)
	}

	sctx, cancel := signal.NotifyContext(env.Context(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	srv := storeservice.New(storeservice.Config{
		Address:        listenAddr,
		Store:          bs,
		Buffer:         buf,
		Compress:       flags.Compress,
		EncryptionKey:  encryptionKey,
		CacheSizeBytes: flags.CacheSize << 20,
		MethodPrefix:   rs.Prefix,
		ReadOnly:       flags.ReadOnly,
		Logf:           log.Printf,
	})
	srv.Root().Metrics().Set("blobd", newServerMetrics(sctx, rs.Spec, buf))

	if err := srv.Start(sctx); err != nil {
		return fmt.Errorf("start server: %w", err)
	}
	go func() {
		<-sctx.Done()
		log.Print("Received signal, closing listener")
	}()
	return srv.Wait()
}

func runKeyGen(env *command.Env, keyFile string) error {
	const keyBytes = 32 // suitable for AES-256 and chacha20poly1305.

	pp, err := getpass.Prompt("Passphrase: ")
	if err != nil {
		return err
	}
	if cf, err := getpass.Prompt("(confirm) Passphrase: "); err != nil {
		return err
	} else if cf != pp {
		return errors.New("passphrases do not match")
	}
	kf := keyfile.New()
	if _, err := kf.Random(pp, keyBytes); err != nil {
		return fmt.Errorf("generate key: %w", err)
	}
	if err := atomicfile.WriteData(keyFile, kf.Encode(), 0600); err != nil {
		return fmt.Errorf("write key file: %w", err)
	}
	fmt.Fprintf(env, "Wrote a new %d-byte key to %q\n", keyBytes, keyFile)
	return nil
}

func getListenAddr(env *command.Env) (string, error) {
	s := env.Config.(*config.Settings)
	if flags.ListenAddr == "" && !strings.HasPrefix(s.DefaultStore, "@") {
		return "", env.Usagef("you must provide a non-empty --listen address")
	}
	target := cmp.Or(flags.ListenAddr, s.DefaultStore)
	spec := s.ResolveAddress(target)
	if spec.Address == "" {
		return "", fmt.Errorf("no service address for %q", target)
	}
	return spec.Address, nil
}
