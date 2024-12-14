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

package cmdstorage

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
	"github.com/creachadair/ffs/blob"
	"github.com/creachadair/ffs/storage/cachestore"
	"github.com/creachadair/ffs/storage/codecs/encrypted"
	"github.com/creachadair/ffs/storage/encoded"
	"github.com/creachadair/ffs/storage/wbstore"
	"github.com/creachadair/ffstools/ffs/internal/cmdstorage/registry"
	"github.com/creachadair/ffstools/lib/zstdc"
	"github.com/creachadair/getpass"
	"github.com/creachadair/keyfile"
	"github.com/creachadair/taskgroup"
	"golang.org/x/crypto/chacha20poly1305"
	"golang.org/x/crypto/sha3"
)

type closer = func()

type startConfig struct {
	Address string
	Spec    string
	Store   blob.CAS
	Prefix  string
	Buffer  blob.Store
}

func (s *startConfig) listen(ctx context.Context) (net.Listener, error) {
	return net.Listen(chirp.SplitAddress(s.Address))
}

func startChirpServer(ctx context.Context, opts startConfig) (closer, *taskgroup.Single[error], error) {
	lst, err := opts.listen(ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("listen: %w", err)
	}
	isUnix := lst.Addr().Network() == "unix"
	if isUnix {
		os.Chmod(opts.Address, 0600) // best-effort
	}
	log.Printf("[chirp] Service: %q", opts.Address)

	service := chirpstore.NewService(opts.Store, &chirpstore.ServiceOpts{Prefix: opts.Prefix})
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
	}, loop, nil
}

func openStore(ctx context.Context, storeSpec string) (cas blob.CAS, buf blob.Store, oerr error) {
	defer func() {
		if x := recover(); x != nil {
			panic(x)
		}
		if buf != nil {
			cas = wbstore.New(ctx, cas, buf)
		}
		if flags.CacheSize > 0 {
			cas = cachestore.NewCAS(cas, flags.CacheSize<<20)
		}
	}()

	bs, err := registry.Stores.Open(ctx, storeSpec)
	if err != nil {
		return nil, nil, fmt.Errorf("open store: %w", err)
	}
	defer closeOnError(bs, &oerr)

	if flags.ReadOnly {
		bs = roStore{bs}
	}

	if flags.BufferDB != "" {
		buf, err = registry.Stores.Open(ctx, flags.BufferDB)
		if err != nil {
			return nil, nil, fmt.Errorf("open buffer: %w", err)
		}
		defer closeOnError(buf, &oerr)
	}
	if flags.Compress {
		bs = encoded.New(bs, zstdc.New())
	}
	if flags.KeyFile == "" {
		if flags.SignKeys {
			log.Print("WARNING: Ignoring --sign-keys because --key is unset")
		}
		return blob.NewCAS(bs, sha3.New256), buf, nil
	}

	key, err := getEncryptionKey(flags.KeyFile)
	if err != nil {
		return nil, nil, fmt.Errorf("get encryption key: %w", err)
	}
	var aead cipher.AEAD
	switch strings.ToLower(flags.Cipher) {
	case "aes", "gcm", "aes256-gcm":
		c, err := aes.NewCipher(key)
		if err != nil {
			return nil, nil, fmt.Errorf("create cipher: %w", err)
		}
		aead, err = cipher.NewGCM(c)
		if err != nil {
			return nil, nil, err
		}
	case "chacha", "chacha20-poly1305":
		aead, err = chacha20poly1305.NewX(key)
		if err != nil {
			return nil, nil, err
		}
	default:
		return nil, nil, fmt.Errorf("unknown cipher %q", flags.Cipher)
	}

	hcons := sha3.New256
	if flags.SignKeys {
		hcons = func() hash.Hash { return hmac.New(sha3.New256, key) }
	}
	bs = encoded.New(bs, encrypted.New(aead, nil))
	return blob.NewCAS(bs, hcons), buf, nil
}

func expvarString(s string) *expvar.String { v := new(expvar.String); v.Set(s); return v }

func expvarInt(z int) *expvar.Int { v := new(expvar.Int); v.Set(int64(z)); return v }

type expvarBool bool

func (b expvarBool) String() string { return strconv.FormatBool(bool(b)) }

func newServerMetrics(ctx context.Context, opts startConfig) *expvar.Map {
	mx := new(expvar.Map)
	mx.Set("started", expvarString(time.Now().UTC().Format(time.RFC3339)))
	mx.Set("store", expvarString(opts.Spec))
	mx.Set("pid", expvarInt(os.Getpid()))
	mx.Set("writable", expvarBool(!flags.ReadOnly))
	mx.Set("encrypted", expvarBool(flags.KeyFile != ""))
	if flags.KeyFile != "" {
		mx.Set("keyfile", expvarString(flags.KeyFile))
		mx.Set("signKeys", expvarBool(flags.SignKeys))
	}
	mx.Set("compressed", expvarBool(flags.Compress))
	mx.Set("cache_size", expvarInt(flags.CacheSize))
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
		mx.Set("buffer_db", expvarString(flags.BufferDB))
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
	data, err := os.ReadFile(keyFile)
	if err != nil {
		return nil, err
	}
	kf, err := keyfile.Parse(data)
	if err != nil {
		if len(data) >= 16 {
			return data, nil
		}
		return nil, fmt.Errorf("invalid key file: %w", err)
	}
	pp, ok := os.LookupEnv("FFS_PASSPHRASE")
	if !ok {
		pp, err = getpass.Prompt("Passphrase: ")
		if err != nil {
			return nil, err
		}
	}
	return kf.Get(pp)
}

func closeOnError(c interface{ Close(context.Context) error }, errp *error) func() {
	return func() {
		if *errp != nil {
			c.Close(context.Background())
		}
	}
}
