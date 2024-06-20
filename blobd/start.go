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
	"cmp"
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/hmac"
	"errors"
	"expvar"
	"fmt"
	"hash"
	"io"
	"log"
	"net"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/creachadair/chirp"
	"github.com/creachadair/chirp/peers"
	"github.com/creachadair/chirpstore"
	"github.com/creachadair/command"
	"github.com/creachadair/ctrl"
	"github.com/creachadair/ffs/blob"
	"github.com/creachadair/ffs/storage/cachestore"
	"github.com/creachadair/ffs/storage/codecs/encrypted"
	"github.com/creachadair/ffs/storage/encoded"
	"github.com/creachadair/ffs/storage/wbstore"
	"github.com/creachadair/ffstools/lib/zstdc"
	"github.com/creachadair/keyfile"
	"github.com/creachadair/taskgroup"
	"golang.org/x/crypto/chacha20poly1305"
	"golang.org/x/crypto/hkdf"
	"golang.org/x/crypto/sha3"
	"golang.org/x/term"
)

type closer = func()

type startConfig struct {
	Address string
	Store   blob.CAS
	Buffer  blob.Store
}

func (s *startConfig) listen(ctx context.Context) (net.Listener, error) {
	return net.Listen(chirp.SplitAddress(s.Address))
}

func startChirpServer(ctx context.Context, opts startConfig) (closer, *taskgroup.Single[error]) {
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
	loop := taskgroup.Go(func() error {
		return peers.Loop(ctx, peers.NetAccepter(lst), func() *chirp.Peer {
			p := chirp.NewPeer()
			p.Metrics().Set("blobd", mx)
			service.Register(p)
			return p
		})
	})

	return func() {
		lst.Close()
		if isUnix {
			defer os.Remove(opts.Address)
		}
	}, loop
}

func mustOpenStore(ctx context.Context) (cas blob.CAS, buf blob.Store) {
	defer func() {
		if x := recover(); x != nil {
			panic(x)
		}
		if buf != nil {
			cas = wbstore.New(ctx, cas, buf)
		}
		if cacheSize > 0 {
			cas = cachestore.NewCAS(cas, cacheSize<<20)
		}
	}()

	bs, err := stores.Open(ctx, storeAddr)
	if err != nil {
		ctrl.Fatalf("Opening store: %v", err)
	}
	if doReadOnly {
		bs = roStore{bs}
	}

	if bufferDB != "" {
		buf, err = stores.Open(ctx, bufferDB)
		if err != nil {
			ctrl.Fatalf("Opening buffer store: %v", err)
		}
	}
	if doCompress {
		bs = encoded.New(bs, zstdc.New())
	}
	if keyFile == "" {
		if doSignKeys {
			log.Print("WARNING: Ignoring --sign-keys because --keyfile is unset")
		}
		return blob.NewCAS(bs, sha3.New256), buf
	}

	key, err := getEncryptionKey(keyFile)
	if err != nil {
		ctrl.Fatalf("Error: %v", err)
	}
	var aead cipher.AEAD
	switch strings.ToLower(aeadStyle) {
	case "aes", "gcm", "aes256-gcm":
		c, err := aes.NewCipher(key)
		if err != nil {
			ctrl.Fatalf("Creating cipher: %v", err)
		}
		aead, err = cipher.NewGCM(c)
		if err != nil {
			ctrl.Fatalf("Creating GCM instance: %v", err)
		}
	case "chacha", "chacha20-poly1305":
		aead, err = chacha20poly1305.NewX(key)
		if err != nil {
			ctrl.Fatalf("Creating chacha20-poly1305 instance: %v", err)
		}
	default:
		ctrl.Fatalf("Unknown encryption algorithm %q", aeadStyle)
	}

	hcons := sha3.New256
	if doSignKeys {
		hcons = func() hash.Hash { return hmac.New(sha3.New256, key) }
	}
	bs = encoded.New(bs, encrypted.New(aead, nil))
	return blob.NewCAS(bs, hcons), buf
}

func expvarString(s string) *expvar.String { v := new(expvar.String); v.Set(s); return v }

func expvarInt(z int) *expvar.Int { v := new(expvar.Int); v.Set(int64(z)); return v }

type expvarBool bool

func (b expvarBool) String() string { return strconv.FormatBool(bool(b)) }

func newServerMetrics(ctx context.Context, opts startConfig) *expvar.Map {
	mx := new(expvar.Map)
	mx.Set("started", expvarString(time.Now().UTC().Format(time.RFC3339)))
	mx.Set("store", expvarString(storeAddr))
	mx.Set("pid", expvarInt(os.Getpid()))
	mx.Set("writable", expvarBool(!doReadOnly))
	mx.Set("encrypted", expvarBool(keyFile != ""))
	if keyFile != "" {
		mx.Set("keyfile", expvarString(keyFile))
		mx.Set("signKeys", expvarBool(doSignKeys))
	}
	mx.Set("compressed", expvarBool(doCompress))
	mx.Set("cache_size", expvarInt(cacheSize))
	if vi := command.GetVersionInfo(); true {
		v := new(expvar.Map)
		v.Set("go_version", expvarString(vi.Toolchain))
		v.Set("package", expvarString(vi.ImportPath))
		v.Set("revision", expvarString(cmp.Or(vi.Commit, vi.Version, "[unknown]")))
		v.Set("modified", expvarBool(vi.Modified))
		if vi.Time != nil {
			v.Set("build_time", expvarString(vi.Time.Format(time.RFC3339)))
		}
		mx.Set("build_info", v)
	}

	if opts.Buffer != nil {
		mx.Set("buffer_db", expvarString(bufferDB))
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

type roStore struct {
	blob.Store
}

var errReadOnlyStore = errors.New("storage is read-only")

func (roStore) Put(context.Context, blob.PutOptions) error { return errReadOnlyStore }
func (roStore) Delete(context.Context, string) error       { return errReadOnlyStore }

func getEncryptionKey(keyFile string) ([]byte, error) {
	if tail, ok := strings.CutPrefix(keyFile, "@@"); ok {
		keyFile = "@" + tail // unescape leading "@"
	} else if tail, ok := strings.CutPrefix(keyFile, "@"); ok {
		if tail == "" {
			return nil, errors.New("key generation salt is empty")
		}
		pp, ok := os.LookupEnv("BLOBD_KEYFILE_PASSPHRASE")
		if !ok {
			io.WriteString(os.Stdout, "Passphrase: ")
			bits, err := term.ReadPassword(0)
			if err != nil {
				return nil, fmt.Errorf("read passphrase: %w", err)
			}
			pp = string(bits)
		}

		hr := hkdf.New(sha3.New256, []byte(pp), []byte(tail), nil)
		var buf [32]byte
		if _, err := io.ReadFull(hr, buf[:]); err != nil {
			return nil, fmt.Errorf("generate key: %w", err)
		}
		return buf[:], nil
	}

	key, err := keyfile.LoadKey(keyFile, func() (string, error) {
		pp, ok := os.LookupEnv("BLOBD_KEYFILE_PASSPHRASE")
		if ok {
			return pp, nil
		}
		io.WriteString(os.Stdout, "Passphrase: ")
		bits, err := term.ReadPassword(0)
		return string(bits), err
	})
	if err != nil {
		return nil, fmt.Errorf("load encryption key: %w", err)
	}
	return key, nil
}
