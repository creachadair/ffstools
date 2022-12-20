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

package main

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/hmac"
	"expvar"
	"hash"
	"io"
	"log"
	"os"
	"runtime/debug"
	"strconv"

	"github.com/creachadair/chirp"
	"github.com/creachadair/chirp/peers"
	"github.com/creachadair/chirpstore"
	"github.com/creachadair/ctrl"
	"github.com/creachadair/ffs/blob"
	"github.com/creachadair/ffs/storage/cachestore"
	"github.com/creachadair/ffs/storage/codecs/encrypted"
	"github.com/creachadair/ffs/storage/codecs/zlib"
	"github.com/creachadair/ffs/storage/encoded"
	"github.com/creachadair/ffs/storage/wbstore"
	"github.com/creachadair/keyfile"
	"golang.org/x/crypto/sha3"
	"golang.org/x/term"
)

type closer = func()

type startConfig struct {
	Address string
	Store   blob.CAS
	Buffer  blob.Store
}

func startChirpServer(ctx context.Context, opts startConfig) (closer, <-chan error) {
	lst, err := opts.listen(ctx)
	if err != nil {
		ctrl.Fatalf("Listen: %v", err)
	}
	isUnix := lst.Addr().Network() == "unix"
	if isUnix {
		os.Chmod(opts.Address, 0600) // best-effort
	}
	log.Printf("[chirp] Service: %q", opts.Address)

	service := chirpstore.NewService(opts.Store, nil)
	mx := newServerMetrics(ctx, opts)
	errc := make(chan error, 1)
	go func() {
		defer close(errc)
		errc <- peers.Loop(ctx, peers.NetAccepter(lst), func() *chirp.Peer {
			p := chirp.NewPeer()
			p.Metrics().Set("blobd", mx)
			service.Register(p)
			return p
		})
	}()

	return func() {
		lst.Close()
		if isUnix {
			defer os.Remove(opts.Address)
		}
	}, errc
}

func mustOpenStore(ctx context.Context) (cas blob.CAS, buf blob.Store) {
	defer func() {
		if x := recover(); x != nil {
			panic(x)
		}
		if buf != nil {
			cas = wbstore.New(ctx, cas, buf)
		}
		if *cacheSize > 0 {
			cas = cachestore.NewCAS(cas, *cacheSize<<20)
		}
	}()

	bs, err := stores.Open(ctx, *storeAddr)
	if err != nil {
		ctrl.Fatalf("Opening store: %v", err)
	}

	if *bufferDB != "" {
		buf, err = stores.Open(ctx, *bufferDB)
		if err != nil {
			ctrl.Fatalf("Opening buffer store: %v", err)
		}
	}
	if *zlibLevel > 0 {
		bs = encoded.New(bs, zlib.NewCodec(zlib.Level(*zlibLevel)))
	}
	if *keyFile == "" {
		return blob.NewCAS(bs, sha3.New256), buf
	}

	key, err := keyfile.LoadKey(*keyFile, func() (string, error) {
		io.WriteString(os.Stdout, "Passphrase: ")
		bits, err := term.ReadPassword(0)
		return string(bits), err
	})
	if err != nil {
		ctrl.Fatalf("Loading encryption key: %v", err)
	}

	c, err := aes.NewCipher(key)
	if err != nil {
		ctrl.Fatalf("Creating cipher: %v", err)
	}
	gcm, err := cipher.NewGCM(c)
	if err != nil {
		ctrl.Fatalf("Creating GCM instance: %v", err)
	}
	bs = encoded.New(bs, encrypted.New(gcm, nil))
	return blob.NewCAS(bs, func() hash.Hash {
		return hmac.New(sha3.New256, key)
	}), buf
}

func expvarString(s string) *expvar.String { v := new(expvar.String); v.Set(s); return v }

func expvarInt(z int) *expvar.Int { v := new(expvar.Int); v.Set(int64(z)); return v }

type expvarBool bool

func (b expvarBool) String() string { return strconv.FormatBool(bool(b)) }

func newServerMetrics(ctx context.Context, opts startConfig) *expvar.Map {
	mx := new(expvar.Map)
	mx.Set("store", expvarString(*storeAddr))
	mx.Set("pid", expvarInt(os.Getpid()))
	mx.Set("encrypted", expvarBool(*keyFile != ""))
	if *keyFile != "" {
		mx.Set("keyfile", expvarString(*keyFile))
	}
	mx.Set("compressed", expvarBool(*zlibLevel > 0))
	mx.Set("cache_size", expvarInt(*cacheSize))
	if bi := getBuildInfo(); bi != nil {
		v := new(expvar.Map)
		v.Set("go_version", expvarString(bi.toolchain))
		v.Set("package", expvarString(bi.path))
		v.Set("revision", expvarString(bi.revision))
		v.Set("build_time", expvarString(bi.buildTime))
		v.Set("modified", expvarBool(bi.modified))
		mx.Set("build_info", v)
	}

	if opts.Buffer != nil {
		mx.Set("buffer_db", expvarString(*bufferDB))
		mx.Set("buffer_len", expvar.Func(func() any {
			n, err := opts.Buffer.Len(ctx)
			if err != nil {
				return "unknown"
			}
			return strconv.FormatInt(n, 10)
		}))
	}
	return mx
}

type buildInfo struct {
	toolchain, path string
	revision        string
	buildTime       string
	modified        bool
}

func first(ss ...string) string {
	for _, s := range ss {
		if s != "" {
			return s
		}
	}
	return ""
}

func getBuildInfo() *buildInfo {
	bi, ok := debug.ReadBuildInfo()
	if !ok {
		return nil
	}
	out := &buildInfo{
		toolchain: first(bi.GoVersion, "[unknown toolchain]"),
		path:      first(bi.Path, "[unknown path]"),
		buildTime: "[unknown build time]",
		revision:  "[unknown revision]",
	}
	for _, s := range bi.Settings {
		switch s.Key {
		case "vcs.revision":
			out.revision = s.Value
		case "vcs.time":
			out.buildTime = s.Value
		case "vcs.modified":
			out.modified = (s.Value != "false")
		}
	}
	return out
}
