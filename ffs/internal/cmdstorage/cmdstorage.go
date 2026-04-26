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
	"io"
	"log"
	"os"
	"os/exec"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/creachadair/atomicfile"
	"github.com/creachadair/chirp"
	"github.com/creachadair/chirp/peers"
	"github.com/creachadair/command"
	"github.com/creachadair/ffs/blob"
	"github.com/creachadair/ffstools/ffs/config"
	"github.com/creachadair/ffstools/ffs/internal/cmdstorage/registry"
	"github.com/creachadair/ffstools/lib/pipestore"
	"github.com/creachadair/ffstools/lib/storeservice"
	"github.com/creachadair/flax"
	"github.com/creachadair/getpass"
	"github.com/creachadair/keyring"
	"golang.org/x/crypto/chacha20poly1305"
)

var flags struct {
	ListenAddr string `flag:"listen,Service address (required)"`
	KeyFile    string `flag:"key,Encryption key file (if empty, do not encrypt)"`
	BufferDB   string `flag:"buffer,Write-behind buffer database"`
	CacheSize  int    `flag:"cache,Memory cache size in MiB (0 means no cache)"`
	Compress   bool   `flag:"compress,Enable zstd compression of blob data"`
	ReadOnly   bool   `flag:"read-only,Disallow modification of the store"`
	Exec       bool   `flag:"exec,Execute a command, then stop the storage service and exit"`
}

var Command = &command.C{
	Name:  "storage",
	Usage: "--store <spec> --listen <addr> [options]",
	Help: fmt.Sprintf(`Run a storage server.

Serve content from the blob.Store described by the --store spec.
The server listens at the --listen address, which may be either
a host:port or the path of a Unix-domain socket.

A store spec is a storage type and address: type:address
The types understood are: %[1]s

If --store has the form "@name", the storage spec associated with that
name in the FFS config file is used (if defined). If the name has the
form "@name+sub", or if the --substore flag is set, only the specified
substore of that base store is exported.

The --listen flag must be one of:

 - A label of the form "@name": The address associated with that name
   in the FFS config file is used. If --listen is empty and the --store
   flag has the form "@name", it uses the address from that setting.

 - A host:port address: A TCP listener is created at that address.

 - Otherwise: The path for a Unix-domain socket.

With --cache, the server provides a memory cache over the primary store.

With --key, the store is opened with chacha20-poly1305 encryption.
The contents of the --key file are used as the cipher key.
If the file is in the [keyring] format, it is unlocked with a passphrase.
The passphrase is read from the FFS_PASSPHRASE environment or prompted at
the terminal. Otherwise if it is exactly 32 bytes (the size of a
chacha20-poly1305 key) its contents are used verbatim.

Use --buffer to enable a local write-behind buffer. The syntax of its
argument is the same as for --store. This is suitable for primary stores
that are remote and slow (e.g., cloud storage).

If --exec is set, the remaining non-flag arguments are used as the name
and arguments of a command to execute as a subprocess. In this mode, the
storage server exits once the subprocess exits. If --listen is set, the
service listens at that address in the usual way; otherwise it exports
the service via a pipe rather than a network address. In either case,
the server sets the FFS_STORE environment variable to the target address.

When serving over a pipe, the address format is:

   _pipe:<r>:<w>

where <r> is the read descriptor ID and <w> the write descriptor ID.

[keyring]: http://godoc.org/github.com/creachadair/keyring`,
		strings.Join(registry.Stores.Names(), ", ")),

	SetFlags: command.Flags(flax.MustBind, &flags),

	// Disable flag merging for this subcommand, so that we will not pluck
	// arguments from the arguments of the nested subcommand.
	CustomFlags: true,
	Init: func(env *command.Env) error {
		return env.MergeFlags(false).ParseFlags()
	},

	Run: command.Adapt(runStorage),

	Commands: []*command.C{{
		Name:     "keygen",
		Usage:    "<key-file>",
		Help:     "Generate a random encryption key into the specified file.",
		SetFlags: command.Flags(flax.MustBind, &keyGenFlags),
		Run:      command.Adapt(runKeyGen),
	}},
}

func runStorage(env *command.Env, execArgs []string) error {
	switch {
	case flags.Exec && len(execArgs) == 0:
		return env.Usagef("missing exec command")
	case !flags.Exec && len(execArgs) != 0:
		return env.Usagef("extra arguments after command: %q", execArgs)
	case !isStoreFlagSet(env):
		return env.Usagef("the --store flag must be set")
	}
	s := env.Config.(*config.Settings)
	rs := s.ResolveSpec(s.DefaultStore)
	if rs.Spec == "" {
		return env.Usagef("you must provide a --store spec")
	}
	listenAddr, err := getListenAddr(env)
	if err != nil {
		return err
	}
	keys, err := getEncryptionKey(flags.KeyFile)
	if err != nil {
		return err
	}
	bs, buf, err := openStore(env.Context(), rs)
	if err != nil {
		return err
	}
	defer func() {
		// N.B. Invoke close with a fresh context, since the parent is likely to
		// have been already canceled during shutdown. Set a timeout in case the
		// close gets stuck, however.
		cctx, cancel := context.WithTimeout(env.Context(), 5*time.Second)
		defer cancel()
		if c, ok := buf.(blob.Closer); ok {
			if err := c.Close(cctx); err != nil {
				log.Printf("Warning: closing buffer: %v", err)
			}
		}
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

	sub, err := maybeInitSubprocess(sctx, listenAddr, execArgs)
	if err != nil {
		return err
	}

	cfg := storeservice.Config{
		Address:        listenAddr,
		Accept:         sub.Accept,
		Store:          bs,
		Buffer:         buf,
		Compress:       flags.Compress,
		Keyring:        keys,
		CacheSizeBytes: flags.CacheSize << 20,
		MethodPrefix:   rs.Prefix,
		ReadOnly:       flags.ReadOnly,
		Logf:           log.Printf,
	}

	srv := storeservice.New(cfg)
	srv.Root().Metrics().Set("blobd", newServerMetrics(sctx, rs.Spec, srv))

	// Now we are ready to start the storage service....
	if err := srv.Start(sctx); err != nil {
		cancel()
		return fmt.Errorf("start server: %w", err)
	}
	if err := sub.Start(); err != nil {
		cancel()
		return fmt.Errorf("start subprocess: %w", err)
	}

	select {
	case <-sctx.Done():
		log.Print("Received signal, stopping storage service")
	case err := <-sub.Errc: // never ready unless there is a subprocess
		if err != nil {
			log.Printf("Error from subprocess: %v", err)
		}
		cancel()
	}
	return srv.Wait()
}

type subprocess struct {
	// For the caller.
	Name   string                                       // for display purposes
	Errc   chan error                                   // from exec.Cmd.Wait
	Accept func(context.Context) (chirp.Channel, error) // if using a pipe

	// Internal plumbing.
	cmd   *exec.Cmd
	conns peers.AcceptChan
	sc    chirp.Channel
}

// maybeInitSubprocess checks whether we have a subprocess to execute. If so,
// the Errc field of the result will be non-nil so it can report an error in
// the select below. If not, Errc will remain nil, and thus we will wait only
// for the service itself.
func maybeInitSubprocess(ctx context.Context, listenAddr string, execArgs []string) (subprocess, error) {
	if !flags.Exec {
		return subprocess{}, nil
	}

	name, rest := execArgs[0], execArgs[1:]
	cmd := exec.CommandContext(ctx, name, rest...)
	cmd.Stdin = os.Stdin
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	errc := make(chan error, 1)
	sub := subprocess{Name: name, Errc: errc, cmd: cmd}
	if listenAddr != "" {
		cmd.Env = append(os.Environ(), "FFS_STORE="+listenAddr)
	} else {
		// Construct a pipe.
		sc, cr, cw, err := pipestore.Connect()
		if err != nil {
			return subprocess{}, fmt.Errorf("connect subprocess: %w", err)
		}
		sub.conns = make(peers.AcceptChan)
		sub.sc = sc
		sub.Accept = sub.conns.Accept

		// Tell the subprocess about the service. Note that we do not use the
		// descriptor IDs from r and w directly, since they will change after
		// exec.  Go promises the extra files will be numbered from 3.
		cmd.Env = append(os.Environ(), "FFS_STORE=_pipe:3:4")
		cmd.ExtraFiles = []*os.File{cr, cw}
	}
	return sub, nil
}

func (s subprocess) Start() error {
	if s.cmd == nil {
		return nil // nothing to do
	}

	log.Printf("Starting subprocess %q", s.Name)
	if err := s.cmd.Start(); err != nil {
		return fmt.Errorf("start subprocess: %w", err)
	}
	// These are owned by the child process now, close our dups.
	for _, f := range s.cmd.ExtraFiles {
		f.Close()
	}
	if s.conns != nil {
		s.conns <- s.sc
	}
	go func() {
		defer close(s.Errc)
		s.Errc <- s.cmd.Wait()
	}()
	return nil
}

var keyGenFlags struct {
	Bare bool `flag:"bare,Generate a bare key, not stored in a keyring (UNSAFE)"`
}

func runKeyGen(env *command.Env, keyFile string) error {
	if _, err := os.Stat(keyFile); err == nil {
		return fmt.Errorf("key file %q already exists", keyFile)
	}

	const keyBytes = chacha20poly1305.KeySize
	if keyGenFlags.Bare {
		if err := atomicfile.Tx(keyFile, 0600, func(w io.Writer) error {
			_, err := w.Write(keyring.RandomKey(keyBytes))
			return err
		}); err != nil {
			return fmt.Errorf("write key file: %w", err)
		}
		fmt.Fprintf(env, "Wrote an unencrypted %d-byte key to %q\n", keyBytes, keyFile)
		return nil
	}

	pp, err := getpass.Prompt("Passphrase: ")
	if err != nil {
		return err
	}
	if cf, err := getpass.Prompt("(confirm) Passphrase: "); err != nil {
		return err
	} else if cf != pp {
		return errors.New("passphrases do not match")
	}

	accessKey, accessKeySalt := keyring.AccessKeyFromPassphrase(pp)
	kr, err := keyring.New(keyring.Config{
		InitialKey:    keyring.RandomKey(keyBytes),
		AccessKey:     accessKey,
		AccessKeySalt: accessKeySalt,
	})
	if err != nil {
		return err
	}
	if err := atomicfile.Tx(keyFile, 0600, func(w io.Writer) error {
		_, err := kr.WriteTo(w)
		return err
	}); err != nil {
		return fmt.Errorf("write keyring: %w", err)
	}
	fmt.Fprintf(env, "Wrote a new %d-byte keyring to %q\n", keyBytes, keyFile)
	return nil
}

func getListenAddr(env *command.Env) (string, error) {
	s := env.Config.(*config.Settings)
	if flags.ListenAddr == "" {
		if flags.Exec {
			return "", nil // use a pipe
		} else if !strings.HasPrefix(s.DefaultStore, "@") {
			return "", env.Usagef("you must provide a non-empty --listen address")
		}
	}
	target := cmp.Or(flags.ListenAddr, s.DefaultStore)
	spec := s.ResolveAddress(target)
	if spec.Address == "" {
		return "", fmt.Errorf("no service address for %q", target)
	}
	return spec.Address, nil
}

func isStoreFlagSet(env *command.Env) bool {
	for env.Parent != nil {
		env = env.Parent
	}
	return env.IsFlagSet("store")
}
