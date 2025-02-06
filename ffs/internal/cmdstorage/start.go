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
	"expvar"
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/creachadair/command"
	"github.com/creachadair/ffs/blob"
	"github.com/creachadair/ffstools/ffs/internal/cmdstorage/registry"
	"github.com/creachadair/ffstools/lib/storeservice"
	"github.com/creachadair/getpass"
	"github.com/creachadair/keyfile"
)

func openStore(ctx context.Context, storeSpec string) (bs, buf blob.StoreCloser, oerr error) {
	// Open the primary store.
	bs, err := registry.Stores.Open(ctx, storeSpec)
	if err != nil {
		return nil, nil, fmt.Errorf("open store: %w", err)
	}
	if flags.BufferDB == "" {
		return bs, nil, nil
	}
	defer closeOnError(bs, &oerr)

	// Open a KV on the write-behind store.
	buf, berr := registry.Stores.Open(ctx, flags.BufferDB)
	if berr != nil {
		return nil, nil, fmt.Errorf("open buffer: %w", berr)
	}
	return bs, buf, nil
}

func expvarString(s string) *expvar.String { v := new(expvar.String); v.Set(s); return v }

func expvarInt(z int) *expvar.Int { v := new(expvar.Int); v.Set(int64(z)); return v }

type expvarBool bool

func (b expvarBool) String() string { return strconv.FormatBool(bool(b)) }

func newServerMetrics(ctx context.Context, spec string, srv *storeservice.Service) *expvar.Map {
	mx := new(expvar.Map)
	mx.Set("started", expvarString(time.Now().UTC().Format(time.RFC3339)))
	mx.Set("store", expvarString(spec))
	mx.Set("pid", expvarInt(os.Getpid()))
	mx.Set("writable", expvarBool(!flags.ReadOnly))
	mx.Set("encrypted", expvarBool(flags.KeyFile != ""))
	if flags.KeyFile != "" {
		mx.Set("keyfile", expvarString(flags.KeyFile))
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

	if flags.BufferDB != "" {
		mx.Set("buffer_db", expvarString(flags.BufferDB))
		mx.Set("buffer_len", expvar.Func(func() any { return srv.BufferLen() }))
	}
	return mx
}

func getEncryptionKey(keyFile string) ([]byte, error) {
	if keyFile == "" {
		return nil, nil // no key, no error
	}
	data, err := os.ReadFile(keyFile)
	if err != nil {
		return nil, fmt.Errorf("read key file: %w", err)
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
			return nil, fmt.Errorf("read passphrase: %w", err)
		}
	}
	return kf.Get(pp)
}

func closeOnError(c blob.Closer, errp *error) func() {
	return func() {
		if *errp != nil {
			c.Close(context.Background())
		}
	}
}
