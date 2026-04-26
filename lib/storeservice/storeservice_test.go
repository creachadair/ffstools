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

package storeservice_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/creachadair/chirp"
	"github.com/creachadair/chirp/channel"
	"github.com/creachadair/chirpstore"
	"github.com/creachadair/ffs/blob/memstore"
	"github.com/creachadair/ffs/blob/storetest"
	"github.com/creachadair/ffstools/lib/pipestore"
	"github.com/creachadair/ffstools/lib/storeservice"
	"github.com/creachadair/keyring"
	"github.com/creachadair/mds/mnet"
	"github.com/fortytw2/leaktest"
)

func TestService(t *testing.T) {
	defer leaktest.Check(t)()

	store := memstore.New(nil)
	kr, err := keyring.New(keyring.Config{
		AccessKey:  make([]byte, keyring.AccessKeyLen),
		InitialKey: []byte("00000000000000000000000000000000"),
	})
	if err != nil {
		t.Fatalf("New keyring: %v", err)
	}

	network := mnet.New(t.Name())
	srv := storeservice.New(storeservice.Config{
		Address:        "virtual:service",
		Listen:         network.Listen,
		Store:          store,
		CacheSizeBytes: 4096,
		Keyring:        kr,
		Logf:           t.Logf,
	})
	defer srv.Stop()

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	if err := srv.Start(ctx); err != nil {
		t.Fatalf("Start service: %v", err)
	}

	runTest := func(t *testing.T) {
		t.Helper()

		conn, err := network.Dial(chirp.SplitAddress(srv.Address()))
		if err != nil {
			t.Fatalf("Dial service: %v", err)
		}
		defer conn.Close()

		cp := chirp.NewPeer().Start(channel.IO(conn, conn))
		cli := chirpstore.NewStore(cp, nil)

		storetest.Run(t, cli)

		if err := cli.Close(ctx); err != nil {
			t.Errorf("Store close: unexpected error: %v", err)
		}
	}
	t.Run("Check", runTest)

	// Stop and restart the service, and verify that it still works as
	// configured after a restart.
	if err := srv.Stop(); err != nil {
		t.Errorf("Service stop: unexpected error: %v", err)
	}

	if err := srv.Start(ctx); err != nil {
		t.Fatalf("Restart service: %v", err)
	}
	t.Run("Recheck", runTest)

	// Check that when its governing context ends, the service stops cleanly and
	// does not report an error.
	cancel()
	if err := srv.Wait(); err != nil {
		t.Errorf("Service wait: unexpected error: %v", err)
	}
}

func TestPipe(t *testing.T) {
	defer leaktest.Check(t)()

	conns := make(chan chirp.Channel)
	srv := storeservice.New(storeservice.Config{
		Store: memstore.New(nil),
		Logf:  t.Logf,

		// In this configuration we do not provide an Address, to exercise that
		// the Accept hook gets called and used without complaint.
		//
		// For the purpose of the test, we will manually hand channels to the
		// service via the conns channel declared above.
		Accept: func(ctx context.Context) (chirp.Channel, error) {
			select {
			case ch := <-conns:
				return ch, nil
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		},
	})
	defer srv.Stop()

	if err := srv.Start(t.Context()); err != nil {
		t.Fatalf("Start serice: %v", err)
	}

	// Connect a pipe to the store service, and verify we can talk over it from
	// the client side. Under the covers this is the same test as above, but
	// with pipes instead of sockets. Do it multiple times (consecutively) to
	// make sure the service can handle that.
	for i := range 2 {
		t.Run(fmt.Sprintf("Connect-%d", i+1), func(t *testing.T) {
			ch, r, w, err := pipestore.Connect()
			if err != nil {
				t.Fatalf("Connect pipes [#%d]: %v", i+1, err)
			}
			conns <- ch

			cli := pipestore.New(r, w)
			storetest.Run(t, cli)
		})
	}
}
