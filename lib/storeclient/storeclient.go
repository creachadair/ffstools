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

// Package storeclient defines helpers for connecting to a storage service.
//
// # Overview
//
// Given an [Address], use the [Address.Connect] method to connect:
//
//		a := storeclient.Address{
//		   Network:  "unix",
//		   Addr:     "db.sock",
//		   Substore: "foo",
//		}
//
//	   s, err := a.Connect(context.Background())
//
// The context passed to [Address.Connect] is only used to dial and esablish
// the store client. Once the client is created, the caller is responsible for
// closing it.
package storeclient

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"

	"github.com/creachadair/chirp"
	"github.com/creachadair/chirp/channel"
	"github.com/creachadair/chirpstore"
	"github.com/creachadair/ffs/blob"
	"github.com/creachadair/ffs/filetree"
)

// Address is the representation of a store address.
type Address struct {
	Network  string // the network type, e.g., "tcp", "unix", "pipe"
	Addr     string // the target address, e.g., host:port or socket path
	Substore string // the name of the substore to reference
}

// ParseAddress parses s into an [Address].
func ParseAddress(s string) Address {
	spec, substore := s, ""
	if i := strings.LastIndex(s, "+"); i >= 0 {
		spec, substore = s[:i], s[i+1:]
	}
	net, addr := chirp.SplitAddress(spec)
	if fds, ok := strings.CutPrefix(spec, "_pipe:"); ok {
		net, addr = "pipe", fds
	}
	return Address{Network: net, Addr: addr, Substore: substore}
}

// Dial connects to the specified address and returns a [chirp.Channel] to the
// storage service. The provided context affects only the dial, and not the
// lifespan of the resulting channel.
func (a Address) Dial(ctx context.Context) (chirp.Channel, error) {
	if a.Network == "pipe" {
		return a.dialPipe(ctx)
	}
	var d net.Dialer
	conn, err := d.DialContext(ctx, a.Network, a.Addr)
	if err != nil {
		return nil, fmt.Errorf("dialing store: %w", err)
	}
	return channel.IO(conn, conn), nil
}

func (a Address) dialPipe(ctx context.Context) (chirp.Channel, error) {
	pr, pw, ok := strings.Cut(a.Addr, ":")
	if !ok {
		return nil, errors.New("invalid pipe address")
	}
	rfd, err := strconv.ParseInt(pr, 10, 64)
	if err != nil {
		return nil, fmt.Errorf("invalid read descriptor: %w", err)
	}
	wfd, err := strconv.ParseInt(pw, 10, 64)
	if err != nil {
		return nil, fmt.Errorf("invalid write descriptor: %w", err)
	}
	if !isDescriptorValid(uintptr(rfd)) {
		return nil, fmt.Errorf("invalid read fd %d", rfd)
	} else if !isDescriptorValid(uintptr(wfd)) {
		return nil, fmt.Errorf("invalid write fd %d", wfd)
	}
	return channel.ConnectPipe(
		os.NewFile(uintptr(rfd), "read-pipe"),
		os.NewFile(uintptr(wfd), "write-pipe"),
	), nil
}

// Connect connects to the spefied address and returns a [filetree.Store].
// The provided context governs only the connection and initialization of the
// store, and not its lifecycle once created.
func (a Address) Connect(ctx context.Context) (filetree.Store, error) {
	ch, err := a.Dial(ctx)
	if err != nil {
		return filetree.Store{}, err
	}
	peer := chirp.NewPeer().Start(ch)
	bs := chirpstore.NewStore(peer, nil) // TODO(creachadair): Prefix support
	var sub blob.Store = bs
	if a.Substore != "" {
		sub, err = bs.Sub(ctx, a.Substore)
		if err != nil {
			peer.Stop()
			return filetree.Store{}, fmt.Errorf("open substore %q: %w", a.Substore, err)
		}
	}
	return filetree.NewStore(ctx, sub)
}
