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

// Package config defines the configuration settings shared by the
// subcommands of the ffs command-line tool.
package config

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path"
	"strings"

	"github.com/creachadair/chirp"
	"github.com/creachadair/chirp/channel"
	"github.com/creachadair/chirpstore"
	"github.com/creachadair/ffs/blob"
	"github.com/creachadair/ffs/file"
	"github.com/creachadair/ffs/file/root"
	"github.com/creachadair/ffs/file/wiretype"
	"github.com/creachadair/ffs/fpath"
	"github.com/creachadair/ffs/index"
	yaml "gopkg.in/yaml.v3"
)

// DefaultPath is the configuration file path used if not overridden by the
// FFS_CONFIG environment variable.
const DefaultPath = "$HOME/.config/ffs/config.yml"

// Path returns the effective configuration file path. If FFS_CONFIG is set,
// its value is used; otherwise DefaultPath is expanded.
func Path() string {
	if cf, ok := os.LookupEnv("FFS_CONFIG"); ok && cf != "" {
		return cf
	}
	return os.ExpandEnv(DefaultPath)
}

// Settings represents the stored configuration settings for the ffs tool.
type Settings struct {
	// The default address for the blob store service (required).  This must be
	// either a store tag (@name) or an address.
	DefaultStore string `json:"defaultStore" yaml:"default-store"`

	// The default method name prefix to use for the service.
	ServicePrefix string `json:"servicePrefix" yaml:"service-prefix"`

	// The substore name to use within the service.
	Substore string `json:"substore" yaml:"substore"`

	// Enable debug logging for the storage service.
	EnableDebugLogging bool `json:"enableDebugLogging" yaml:"enable-debug-logging"`

	// Well-known store specifications, addressable by tag.
	Stores []*StoreSpec `json:"stores" yaml:"stores"`
}

// A StoreSpec associates a tag (handle) with a storage address.
type StoreSpec struct {
	Tag      string `json:"tag" yaml:"tag"`           // identifies the spec
	Address  string `json:"address" yaml:"address"`   // the listen address
	Spec     string `json:"spec" yaml:"spec"`         // the desired storage URL
	Prefix   string `json:"prefix" yaml:"prefix"`     // service method name prefix
	Substore string `json:"substore" yaml:"substore"` // substore name (optional)
}

// ResolveAddress resolves the given address against the settings. If addr is
// of the form @tag and that tag exists in the settings, the spec for that tag
// is returned; otherwise it returns a spec whose address is addr.
//
// As a special case, if @tag has the form "@tag+name", the name without the
// separator is used as a substore indicator, overriding the config.
//
// As a special case, if the address has a "+name" suffix, the name without the
// separator is used as a substore indicator.
func (s *Settings) ResolveAddress(addr string) StoreSpec {
	tag, ok := strings.CutPrefix(addr, "@")
	if ok {
		sub := s.Substore
		if i := strings.LastIndex(tag, "+"); i >= 0 {
			tag, sub = tag[:i], tag[i+1:]
		}
		for _, st := range s.Stores {
			if tag == st.Tag {
				cp := *st
				ExpandString(&cp.Address)
				if cp.Prefix == "" {
					cp.Prefix = s.ServicePrefix
				}
				if sub != "" {
					cp.Substore = sub
				} else if cp.Substore == "" {
					cp.Substore = s.Substore
				}
				return cp
			}
		}
	} else if i := strings.LastIndex(addr, "+"); i >= 0 {
		return StoreSpec{Address: addr[:i], Prefix: s.ServicePrefix, Substore: addr[i+1:]}
	}
	return StoreSpec{Address: addr, Prefix: s.ServicePrefix, Substore: s.Substore}
}

// ResolveSpec resolves the given store spec against the settings.  If spec is
// of the form @tag and that tag exists in the settings, the expanded form of
// that spec is returned; otherwise it returns a verbatim spec.
func (s *Settings) ResolveSpec(spec string) StoreSpec {
	tag, ok := strings.CutPrefix(spec, "@")
	if ok {
		for _, st := range s.Stores {
			if tag == st.Tag {
				cp := *st
				ExpandString(&cp.Spec)
				if cp.Prefix == "" {
					cp.Prefix = s.ServicePrefix
				}
				return cp
			}
		}
	}
	return StoreSpec{Spec: spec, Prefix: s.ServicePrefix}
}

// OpenStore connects to the store service address in the configuration.  The
// caller is responsible for closing the store when it is no longer needed.
func (s *Settings) OpenStore(ctx context.Context) (Store, error) {
	spec := s.ResolveAddress(s.DefaultStore)
	if spec.Address == "" {
		return Store{}, fmt.Errorf("no store service address (%q)", s.DefaultStore)
	}
	return s.openStoreAddress(ctx, spec)
}

// openStoreAddress connects to the store service at addr.  The caller is
// responsible for closing the store when it is no longer needed.
func (s *Settings) openStoreAddress(ctx context.Context, spec StoreSpec) (Store, error) {
	lg := log.New(log.Writer(), "[ffs] ", log.LstdFlags|log.Lmicroseconds)
	if s.EnableDebugLogging {
		lg.Printf("dial %q", spec.Address)
	}
	conn, err := Dial(chirp.SplitAddress(spec.Address))
	if err != nil {
		return Store{}, fmt.Errorf("dialing store: %w", err)
	}
	peer := chirp.NewPeer().Start(channel.IO(conn, conn))
	if s.EnableDebugLogging {
		peer.LogPackets(func(pkt *chirp.Packet, dir chirp.PacketDir) { lg.Printf("%s %v", dir, pkt) })
	}
	bs := chirpstore.NewStore(peer, &chirpstore.StoreOptions{
		MethodPrefix: spec.Prefix,
	})
	var sub blob.Store = bs
	if spec.Substore != "" {
		sub, err = bs.Sub(ctx, spec.Substore)
		if err != nil {
			conn.Close()
			return Store{}, fmt.Errorf("open substore %q: %w", s.Substore, err)
		}
	}

	rootKV, err := sub.KV(ctx, "root")
	if err != nil {
		conn.Close()
		return Store{}, fmt.Errorf("open root keyspace: %w", err)
	}
	fileKV, err := sub.KV(ctx, "file")
	if err != nil {
		conn.Close()
		return Store{}, fmt.Errorf("open file keyspace: %w", err)
	}
	fileCAS, err := sub.CAS(ctx, "file")
	if err != nil {
		conn.Close()
		return Store{}, fmt.Errorf("open file keyspace: %w", err)
	}
	return Store{
		roots: rootKV,
		files: fileCAS,
		fsync: fileKV,
		s:     sub,
		c:     bs, // N.B. top-level store, for closing
	}, nil
}

// WithStore calls f with a store opened from the configuration. The store is
// closed after f returns. The error returned by f is returned by WithStore.
func (s *Settings) WithStore(ctx context.Context, f func(Store) error) error {
	spec := s.ResolveAddress(s.DefaultStore)
	if spec.Address == "" {
		return fmt.Errorf("no store service address (%q)", s.DefaultStore)
	}
	bs, err := s.openStoreAddress(ctx, spec)
	if err != nil {
		return err
	}
	defer bs.Close(ctx)
	return f(bs)
}

// WithStoreAddress calls f with a store opened at addr. The store is closed
// after f returns. The error returned by f is returned by WithStoreAddress.
func (s *Settings) WithStoreAddress(ctx context.Context, addr string, f func(Store) error) error {
	spec := s.ResolveAddress(addr)
	bs, err := s.openStoreAddress(ctx, spec)
	if err != nil {
		return err
	}
	defer bs.Close(ctx)
	return f(bs)
}

// ParseKey parses the string encoding of a key. A key must be a hex string, a
// base64 string, or a literal string prefixed with "@":
//
//	@foo     encodes "foo"
//	@@foo    encodes "@foo"
//	414243   encodes "ABC"
//	eHl6enk= encodes "xyzzy"
func ParseKey(s string) (string, error) {
	if strings.HasPrefix(s, "@") {
		return s[1:], nil
	}
	var key []byte
	var err error
	if isAllHex(s) {
		key, err = hex.DecodeString(s)
	} else if strings.HasSuffix(s, "=") {
		key, err = base64.StdEncoding.DecodeString(s)
	} else {
		key, err = base64.RawStdEncoding.DecodeString(s) // tolerate missing padding
	}
	if err != nil {
		return "", fmt.Errorf("invalid key %q: %w", s, err)
	}
	return string(key), nil
}

// ExpandString calls os.ExpandEnv to expand environment variables in *s.
// The value of *s is replaced.
func ExpandString(s *string) { *s = os.ExpandEnv(*s) }

// Load reads and parses the contents of a config file from path.  If the
// specified path does not exist, an empty config is returned without error.
func Load(path string) (*Settings, error) {
	data, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return new(Settings), nil
	} else if err != nil {
		return nil, fmt.Errorf("reading config file: %w", err)
	}
	cfg := new(Settings)
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("parsing config file: %w", err)
	}
	return cfg, nil
}

// PrintableKey converts key into a value that will marshal into JSON as a
// sensible human-readable string.
func PrintableKey(key string) any {
	for i := 0; i < len(key); i++ {
		if key[i] < ' ' || key[i] > '~' {
			return base64.StdEncoding.EncodeToString([]byte(key))
		}
	}
	return key
}

// FormatKey converts key into a base64 value.
func FormatKey(key string) string {
	return base64.StdEncoding.EncodeToString([]byte(key))
}

// ToJSON converts a value to indented JSON.
func ToJSON(msg any) string {
	bits, err := json.Marshal(msg)
	if err != nil {
		return "null"
	}
	var buf bytes.Buffer
	if err := json.Indent(&buf, bits, "", "  "); err != nil {
		return "null"
	}
	return buf.String()
}

func isAllHex(s string) bool {
	for _, c := range s {
		if !(c >= '0' && c <= '9' || c >= 'a' && c <= 'f' || c >= 'A' && c <= 'F') {
			return false
		}
	}
	return true
}

// PathInfo is the result of parsing and opening a path spec.
type PathInfo struct {
	Path    string     // the original input path (unparsed)
	Base    *file.File // the root or starting file of the path
	BaseKey string     // the storage key of the base file
	File    *file.File // the target file of the path
	FileKey string     // the storage key of the target file
	Root    *root.Root // the specified root, or nil if none
	RootKey string     // the key of root, or ""
}

// Flush flushes the base file to reflect any changes and returns its updated
// storage key. If p is based on a root, the root is also updated and saved.
func (p *PathInfo) Flush(ctx context.Context) (string, error) {
	key, err := p.Base.Flush(ctx)
	if err != nil {
		return "", err
	}
	p.BaseKey = key

	// If this path started at a root, write out the updated contents.
	if p.Root != nil {
		// If the file has changed, invalidate the index.
		if p.Root.FileKey != key {
			p.Root.IndexKey = ""
		}
		p.Root.FileKey = key
		if err := p.Root.Save(ctx, p.RootKey, true); err != nil {
			return "", err
		}
	}
	return key, nil
}

// OpenPath parses and opens the specified path in s.
// The path has either the form "<root-key>/some/path" or "@<file-key>/some/path".
func OpenPath(ctx context.Context, s Store, path string) (*PathInfo, error) {
	out := &PathInfo{Path: path}

	first, rest := SplitPath(path)

	// Check for a @file key prefix; otherwise it should be a root.
	if !strings.HasPrefix(first, "@") {
		rp, err := root.Open(ctx, s.Roots(), first)
		if err != nil {
			return nil, err
		}
		rf, err := rp.File(ctx, s.Files())
		if err != nil {
			return nil, err
		}
		out.Root = rp
		out.RootKey = first
		out.Base = rf
		out.File = rf
		out.FileKey = rp.FileKey // provisional

	} else if fk, err := ParseKey(strings.TrimPrefix(first, "@")); err != nil {
		return nil, err

	} else if fp, err := file.Open(ctx, s.Files(), fk); err != nil {
		return nil, err

	} else {
		out.Base = fp
		out.File = fp
		out.FileKey = fk
	}
	out.BaseKey = out.Base.Key() // safe, it was just opened

	// If the rest of the path is empty, the starting point is the target.
	if rest == "" {
		return out, nil
	}

	// Otherwise, open a path relative to the base.
	tf, err := fpath.Open(ctx, out.Base, rest)
	if err != nil {
		return nil, err
	}
	out.File = tf
	out.FileKey = out.File.Key() // safe, it was just opened
	return out, nil
}

// SplitPath parses s as a slash-separated path specification.
// The first segment of s identifies the storage key of a root or file, the
// rest indicates a sequence of child names starting from that file.
// The rest may be empty.
func SplitPath(s string) (first, rest string) {
	if pre, post, ok := strings.Cut(s, "=/"); ok { // <base64>=/more/stuff
		return pre + "=", path.Clean(post)
	}
	if strings.HasSuffix(s, "=") {
		return s, ""
	}
	pre, post, _ := strings.Cut(s, "/")
	return pre, path.Clean(post)
}

// Store is a wrapper around a store that adds methods to expose the root and
// data buckets.
type Store struct {
	roots blob.KV
	files blob.CAS
	fsync blob.KV

	s blob.Store
	c blob.Closer
}

// Files returns the files bucket of the underlying storage.
func (s Store) Files() blob.CAS { return s.files }

// Roots returns the roots bucket of the underlying storage.
func (s Store) Roots() blob.KV { return s.roots }

// Sync returns a sync view of the files bucket.
func (s Store) Sync() blob.KV { return s.fsync }

// Store returns the underlying store for c.
func (s Store) Store() blob.Store { return s.s }

// Close closes the store attached to c.
func (s Store) Close(ctx context.Context) error { return s.c.Close(ctx) }

// LoadIndex loads the contents of an index blob.
func LoadIndex(ctx context.Context, s blob.CAS, key string) (*index.Index, error) {
	var obj wiretype.Object
	if err := wiretype.Load(ctx, s, key, &obj); err != nil {
		return nil, fmt.Errorf("loading index: %w", err)
	}
	ridx := obj.GetIndex()
	if ridx == nil {
		return nil, fmt.Errorf("no index in %s", FormatKey(key))
	}

	return index.Decode(ridx)
}
