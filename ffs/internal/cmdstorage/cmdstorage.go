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
	"fmt"
	"log"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/creachadair/command"
	"github.com/creachadair/ffstools/ffs/config"
	"github.com/creachadair/ffstools/ffs/internal/cmdstorage/registry"
	"github.com/creachadair/flax"
)

var flags struct {
	ListenAddr string `flag:"listen,Service address (required)"`
	KeyFile    string `flag:"key,Encryption key file or salt (if empty, do not encrypt)"`
	Cipher     string `flag:"cipher,default=chacha,Encryption algorithm"`
	SignKeys   bool   `flag:"sign-keys,Sign content addresses (requires --key)"`
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

With --key, the store is opened with encryption (chosen by --encryption).
By default, the user-provided passphrase is used to unlock the key file.

If --key begins with "@" or "%%", however, the remaining string is used
as a key salt for HKDF with the user-provided passphrase. When the prefix
is "%%", the user is prompted to confirm the passphrase; with "@" no
confirmation is required. Double the "@" or "%%" to escape this treatment
of the --key argument.

If FFS_PASSPHRASE is set in the environment, it is used as the passphrase
for the key file; otherwise it prompts at the terminal.

Use --buffer to enable a local write-behind buffer. The syntax of its
argument is the same as for --store. This is suitable for primary stores
that are remote and slow (e.g., cloud storage).`,
		strings.Join(registry.Stores.Names(), ", ")),

	SetFlags: command.Flags(flax.MustBind, &flags),
	Run:      command.Adapt(runStorage),
}

func runStorage(env *command.Env) error {
	s := env.Config.(*config.Settings)
	storeSpec := s.DefaultStore
	if storeSpec == "" {
		return env.Usagef("you must provide a --store spec")
	}
	if rs, ok := s.ResolveStoreSpec(storeSpec); ok {
		storeSpec = rs
	}
	listenAddr, err := getListenAddr(env)
	if err != nil {
		return err
	}

	bs, buf, err := openStore(env.Context(), storeSpec)
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
	log.Printf("Store: %q", storeSpec)
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

	config := startConfig{
		Address: listenAddr,
		Spec:    storeSpec,
		Store:   bs,
		Buffer:  buf,
	}

	sctx, cancel := signal.NotifyContext(env.Context(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()
	closer, loop, err := startChirpServer(sctx, config)
	if err != nil {
		return fmt.Errorf("start server: %w", err)
	}
	go func() {
		<-sctx.Done()
		log.Print("Received signal, closing listener")
		closer()
	}()
	return loop.Wait()
}

func getListenAddr(env *command.Env) (string, error) {
	s := env.Config.(*config.Settings)
	if flags.ListenAddr == "" && !strings.HasPrefix(s.DefaultStore, "@") {
		return "", env.Usagef("you must provide a non-empty --listen address")
	}
	target := cmp.Or(flags.ListenAddr, s.DefaultStore)
	addr := s.ResolveAddress(target)
	if addr == "" {
		return "", fmt.Errorf("no service address for %q", target)
	}
	return addr, nil
}
