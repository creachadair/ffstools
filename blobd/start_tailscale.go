//go:build tailscale

// Copyright 2022 Michael J. Fromberger. All Rights Reserved.
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

package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"net/url"
	"strconv"

	"github.com/creachadair/chirp"
	"tailscale.com/tsnet"
)

func parseTailscaleURL(s string) (string, *tsnet.Server, error) {
	u, err := url.Parse(s)
	if err != nil || u.Scheme != "ts" {
		return "", nil, errNotTailscale
	}
	host, port, err := net.SplitHostPort(u.Host)
	if err != nil {
		return "", nil, fmt.Errorf("invalid Tailscale address: %w", err)
	}
	srv := &tsnet.Server{
		Hostname: host,
		Logf:     func(string, ...any) {},
	}
	q := u.Query()
	if s := q.Get("dir"); s != "" {
		srv.Dir = s
	}
	if s := q.Get("auth_key"); s != "" {
		srv.AuthKey = s
	}
	if s := q.Get("ephemeral"); s != "" {
		b, _ := strconv.ParseBool(s)
		srv.Ephemeral = b
	}
	if s := q.Get("verbose"); s != "" {
		if b, _ := strconv.ParseBool(s); b {
			srv.Logf = log.Printf
		}
	}

	return port, srv, nil
}

var errNotTailscale = errors.New("not a Tailscale address")

func (s *startConfig) listen(ctx context.Context) (net.Listener, error) {
	// If the address is a URL in the "ts:" scheme, start a Tailscale node.
	if port, ts, err := parseTailscaleURL(s.Address); err == nil {
		log.Printf("Listening on Tailscale node at %q (%v)", ts.Hostname, port)
		s.Address = "ts://" + ts.Hostname + ":" + port // redacted
		return ts.Listen("tcp", ":"+port)
	} else if !errors.Is(err, errNotTailscale) {
		return nil, err
	}
	return net.Listen(chirp.SplitAddress(s.Address))
}
