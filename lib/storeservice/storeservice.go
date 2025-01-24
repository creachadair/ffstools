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
	"github.com/creachadair/taskgroup"
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

	// Prefix is prepended to the method names exportd by the service.
	// Any caller must use the same prefix.
	Prefix string

	// Logf, if set, is used to write text debug logs.
	// If nil, logs are discarded.
	Logf func(string, ...any)
}

// Service manages a running server, accepting connections and delegating them
// to a peer implementing the [chirpstore.Service] methods.
type Service struct {
	root  *chirp.Peer
	addr  string
	store blob.StoreCloser
	loop  *taskgroup.Single[error]
	stop  func()
	logf  func(string, ...any)
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
	svc := chirpstore.NewService(store, &chirpstore.ServiceOptions{
		Prefix: config.Prefix,
	})
	root := chirp.NewPeer()
	svc.Register(root)
	return &Service{
		root:  root,
		addr:  config.Address,
		store: store,
		logf:  logf,
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
	lst, err := net.Listen(chirp.SplitAddress(s.addr))
	if err != nil {
		return fmt.Errorf("listen: %w", err)
	}
	s.logf("[chirp] service: %q", s.addr)
	s.loop = taskgroup.Go(func() error {
		defer lst.Close()
		return peers.Loop(ctx, peers.NetAccepter(lst), s.root)
	})
	s.stop = func() { lst.Close() }
	return nil
}

// Stop shuts down the service and waits for it to finish.  If s is not
// started, Stop does nothing without error.
func (s *Service) Stop() error {
	if s.loop == nil {
		return nil
	}
	s.stop()
	return s.loop.Wait()
}

// Wait blocks until s has finished running.
func (s *Service) Wait() error { return s.loop.Wait() }

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
