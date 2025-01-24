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
	"net"
	"path/filepath"
	"testing"

	"github.com/creachadair/chirp"
	"github.com/creachadair/chirp/channel"
	"github.com/creachadair/chirpstore"
	"github.com/creachadair/ffs/blob/memstore"
	"github.com/creachadair/ffs/blob/storetest"
	"github.com/creachadair/ffstools/lib/storeservice"
)

func TestService(t *testing.T) {
	store := memstore.New(nil)

	srv := storeservice.New(storeservice.Config{
		Address: filepath.Join(t.TempDir(), "srv.sock"),
		Store:   store,
		Logf:    t.Logf,
	})
	defer srv.Stop()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := srv.Start(ctx); err != nil {
		t.Fatalf("Start service: %v", err)
	}

	conn, err := net.Dial(chirp.SplitAddress(srv.Address()))
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

	cancel()
	if err := srv.Wait(); err != nil {
		t.Logf("Service wait: unexpected error: %v", err)
	}
}
