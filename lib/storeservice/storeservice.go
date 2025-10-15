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

// Package storeservice implements a storage service over Chirp.
package storeservice

import (
	"context"
	"errors"
	"fmt"
	"net"

	"github.com/creachadair/chirp"
	"github.com/creachadair/chirp/peers"
	"github.com/creachadair/chirpstore"
	"github.com/creachadair/ffs/blob"
	"github.com/creachadair/ffs/storage/cachestore"
	"github.com/creachadair/ffs/storage/codecs/encrypted"
	"github.com/creachadair/ffs/storage/encoded"
	"github.com/creachadair/ffs/storage/wbstore"
	"github.com/creachadair/ffstools/lib/zstdc"
	"github.com/creachadair/taskgroup"
	"golang.org/x/crypto/chacha20poly1305"
)

// Config carries the settings for a [Service].
// The Address and Store fields are required.
type Config struct {
	// Address is the address at which the service listens for connections.
	// This must be non-empty.
	Address string

	// Store is the storage exported by the service.
	// This must be non-nil.
	Store blob.StoreCloser

	// ReadOnly, if true, causes methods that modify the store to report errors
	// when called.
	ReadOnly bool

	// Compress, if true, enables zstd compression on the store.
	Compress bool

	// EncryptionKey, if non-empty, is used as an encryption key to wrap the
	// store. Blobs are encrypted using a chacha20-poly1305 AEAD.
	// A valid key must be 32 bytes in length, or New will panic.
	EncryptionKey []byte

	// CacheSizeBytes, if positive, wraps the store in a memory cache of the
	// specified size. If zero or negative, no cache is enabled.
	CacheSizeBytes int

	// Buffer, if non-nil, uses the specified store as a writeback buffer for
	// writes to the underlying store.
	Buffer blob.StoreCloser

	// MethodPrefix is prepended to the method names exported by the service.
	// Any caller must use the same prefix.
	MethodPrefix string

	// Logf, if set, is used to write text debug logs.
	// If nil, logs are discarded.
	Logf func(string, ...any)
}

// Service manages a running server, accepting connections and delegating them
// to a peer implementing the [chirpstore.Service] methods.
type Service struct {
	root       *chirp.Peer
	addr       string
	prefix     string
	store      blob.StoreCloser
	buffer     blob.StoreCloser
	cacheBytes int
	loop       *taskgroup.Single[error]
	stop       context.CancelFunc
	logf       func(string, ...any)
	bufSize    func() int64
}

// New creates a new, unstarted service for the specified config.
// It will panic if any required config fields are missing.
// The caller must call [Service.Start] to start the service.
func New(config Config) *Service {
	switch {
	case config.Address == "":
		panic("missing required listen address")
	case config.Store == nil:
		panic("missing required store")
	}
	logf := config.Logf
	if logf == nil {
		logf = func(string, ...any) {}
	}
	store := config.Store
	if config.ReadOnly {
		store = roStore{config.Store}
	}
	if len(config.EncryptionKey) != 0 {
		aead, err := chacha20poly1305.NewX([]byte(config.EncryptionKey))
		if err != nil {
			panic(fmt.Sprintf("create cipher context: %v", err))
		}
		store = encoded.New(store, encrypted.New(aead, nil))
	}

	// N.B. Compression, if enabled, needs to happen before encryption, since
	// the encryption step makes the data effectively incompressible. However,
	// the encrypted storage wrapper already does compression, so it is not
	// necessary to enable both in most cases.
	if config.Compress {
		store = encoded.New(store, zstdc.New())
	}

	// Enable the cache if requested. But if we have a write-behind buffer,
	// defer this until Start, so that the cache remains frontmost in the stack.
	if config.CacheSizeBytes > 0 && config.Buffer == nil {
		store = cachestore.New(store, config.CacheSizeBytes)
	}

	return &Service{
		root:       chirp.NewPeer(),
		addr:       config.Address,
		prefix:     config.MethodPrefix,
		store:      store,
		buffer:     config.Buffer,
		cacheBytes: config.CacheSizeBytes,
		logf:       logf,
	}
}

// Root returns the root peer for the service. The caller may modify this peer,
// and such changes will affect future connections to the service but will not
// affect existing connections.
func (s *Service) Root() *chirp.Peer { return s.root }

// Address returns the address for the service listener.
func (s *Service) Address() string { return s.addr }

// Start starts up the service loop for s. It will panic if s has already been
// started; otherwise it reports whether startup succeeded.
// Once started, s will run until ctx ends or [Service.Stop] is called.
// Start does not block while the service runs; call [Service.Wait] to wait for
// the service to shut down.
func (s *Service) Start(ctx context.Context) error {
	if s.loop != nil {
		panic("service is already running")
	}

	// If a buffer was enabled, set it up now, so that it runs with the context
	// that will govern the lifecycle of the running service.
	store := s.store
	if s.buffer != nil {
		wb := wbstore.New(ctx, store, s.buffer)

		s.bufSize = func() int64 {
			n, err := wb.BufferLen(ctx)
			if err != nil {
				return -1
			}
			return n
		}
		store = wb

		// If the cache size is positive, it means we want a cache, but we had to
		// defer creating it because of the buffer.
		if s.cacheBytes > 0 {
			store = cachestore.New(store, s.cacheBytes)
		}
	}

	lst, err := net.Listen(chirp.SplitAddress(s.addr))
	if err != nil {
		return fmt.Errorf("listen: %w", err)
	}
	s.logf("[chirp] service: %q", s.addr)

	sctx, cancel := context.WithCancel(ctx)
	s.loop = taskgroup.Go(s.serve(sctx, store, lst))
	s.stop = cancel
	return nil
}

func (s *Service) serve(ctx context.Context, store blob.Store, lst net.Listener) func() error {
	acc := peers.NetAccepter(lst)
	var g taskgroup.Group

	return func() (err error) {
		defer func() {
			lst.Close()

			gerr := g.Wait()
			if err == nil {
				err = gerr
			}
		}()

		for {
			ch, err := acc.Accept(ctx)
			if errors.Is(err, net.ErrClosed) {
				return nil
			} else if err != nil {
				return fmt.Errorf("accept: %w", err)
			}

			peer := s.root.Clone()
			_, store, err := s.checkCaller(ctx, store, ch)
			if err != nil {
				s.logf("reject: %v", err)
				peer.Handle("", reportErrorHandler(err))
			} else {
				svc := chirpstore.NewService(store, &chirpstore.ServiceOptions{Prefix: s.prefix})
				svc.Register(peer)
			}
			peer.Start(ch)
			stop := context.AfterFunc(ctx, func() { peer.Stop() })
			g.Go(func() error {
				defer stop()
				return peer.Wait()
			})
		}
	}
}

// reportErrorHandler returns a [chirp.Handler] that reports the specified
// error to all calls from the remote peer.
func reportErrorHandler(err error) chirp.Handler {
	return func(context.Context, *chirp.Request) ([]byte, error) { return nil, err }
}

func (s *Service) checkCaller(ctx context.Context, store blob.Store, ch chirp.Channel) (string, blob.Store, error) {
	// TODO(creachadair): Assign a starting store based on the caller identity.
	return "", store, nil
}

// BufferLen reports the total number of keys in the buffer store.
// If there is no buffer store, or in case of error, it returns -1.
func (s *Service) BufferLen() int64 {
	if s.bufSize == nil {
		return -1
	}
	return s.bufSize()
}

// Stop shuts down the service and waits for it to finish.  If s is not
// started, Stop does nothing without error. After Stop has returned, s can be
// restarted with a new context.
func (s *Service) Stop() error {
	if s.stop != nil {
		s.stop()
	}
	return s.Wait()
}

// Wait blocks until s has finished running. If s is not running, Wait returns
// immediately without error.  After Wait has returned, s can be restarted with
// a new context.
func (s *Service) Wait() error {
	if s.loop == nil {
		return nil
	}
	defer func() { s.loop = nil }()
	return s.loop.Wait()
}

type roStore struct {
	blob.Store
}

func (r roStore) Close(ctx context.Context) error {
	if c, ok := r.Store.(blob.Closer); ok {
		return c.Close(ctx)
	}
	return nil
}

func (r roStore) KV(ctx context.Context, name string) (blob.KV, error) {
	kv, err := r.Store.KV(ctx, name)
	if err != nil {
		return nil, err
	}
	return roKV{kv}, nil
}

func (r roStore) CAS(ctx context.Context, name string) (blob.CAS, error) {
	cas, err := r.Store.CAS(ctx, name)
	if err != nil {
		return nil, err
	}
	return roCAS{cas}, nil
}

func (r roStore) Sub(ctx context.Context, name string) (blob.Store, error) {
	sub, err := r.Store.Sub(ctx, name)
	if err != nil {
		return nil, err
	}
	return roStore{sub}, nil
}

var errReadOnlyStore = errors.New("storage is read-only")

type roKV struct{ blob.KV }
type roCAS struct{ blob.CAS }

func (roKV) Put(context.Context, blob.PutOptions) error      { return errReadOnlyStore }
func (roCAS) CASPut(context.Context, []byte) (string, error) { return "", errReadOnlyStore }
func (roKV) Delete(context.Context, string) error            { return errReadOnlyStore }
func (roCAS) Delete(context.Context, string) error           { return errReadOnlyStore }
