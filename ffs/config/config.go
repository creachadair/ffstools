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
	"github.com/creachadair/ffs/storage/affixed"
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

	// Enable debug logging for the storage service.
	EnableDebugLogging bool `json:"enableDebugLogging" yaml:"enable-debug-logging"`

	// Well-known store specifications, addressable by tag.
	Stores []*StoreSpec `json:"stores" yaml:"stores"`
}

// A StoreSpec associates a tag (handle) with a storage address.
type StoreSpec struct {
	Tag     string `json:"tag" yaml:"tag"`         // identifies the spec
	Address string `json:"address" yaml:"address"` // the listen address
	Spec    string `json:"spec" yaml:"spec"`       // the desired storage URL
}

// ResolveAddress resolves the given address against the settings.  If addr is
// of the form @tag and that tag exists in the settings, the expanded form of
// the tag is returned; otherwise addr is returned unmodified.
func (s *Settings) ResolveAddress(addr string) string {
	if !strings.HasPrefix(addr, "@") {
		return addr
	}
	tag := strings.TrimPrefix(addr, "@")
	for _, st := range s.Stores {
		if tag == st.Tag {
			ExpandString(&st.Address)
			return st.Address
		}
	}
	return addr
}

// ResolveStoreSpec resolves the given store spec against the settings, and
// reports whether there was a match.  If addr is of the form @tag and that tag
// exists in the settings, the expanded form of that tag's store spec is
// returned; otherwise it returns spec unmodified.
func (s *Settings) ResolveStoreSpec(spec string) (string, bool) {
	tail, ok := strings.CutPrefix(spec, "@")
	if !ok {
		return spec, false
	}
	for _, st := range s.Stores {
		if tail == st.Tag && st.Spec != "" {
			ExpandString(&st.Spec)
			return st.Spec, true
		}
	}
	return spec, false
}

// FindAddress reports whether s has a storage server address, and returns it
// if so. If a tag was selected but not matched, it is returned.
func (s *Settings) FindAddress() (string, bool) {
	if s.DefaultStore == "" {
		return "", false
	} else if strings.HasPrefix(s.DefaultStore, "@") {
		tag := strings.TrimPrefix(s.DefaultStore, "@")
		for _, st := range s.Stores {
			if tag == st.Tag {
				ExpandString(&st.Address)
				return st.Address, true
			}
		}
		return tag, false
	}
	return s.DefaultStore, true
}

// OpenStore connects to the store service address in the configuration.  The
// caller is responsible for closing the store when it is no longer needed.
func (s *Settings) OpenStore(ctx context.Context) (CAS, error) {
	addr, ok := s.FindAddress()
	if !ok {
		return CAS{}, fmt.Errorf("no store service address (%q)", addr)
	}
	return s.OpenStoreAddress(ctx, addr)
}

// OpenStoreAddress connects to the store service at addr.  The caller is
// responsible for closing the store when it is no longer needed.
func (s *Settings) OpenStoreAddress(_ context.Context, addr string) (CAS, error) {
	lg := log.New(log.Writer(), "[ffs] ", log.LstdFlags|log.Lmicroseconds)
	if s.EnableDebugLogging {
		lg.Printf("dial %q", addr)
	}
	conn, err := Dial(chirp.SplitAddress(addr))
	if err != nil {
		return CAS{}, fmt.Errorf("dialing store: %w", err)
	}
	peer := chirp.NewPeer().Start(channel.IO(conn, conn))
	if s.EnableDebugLogging {
		peer.LogPackets(func(pkt *chirp.Packet, dir chirp.PacketDir) { lg.Printf("%s %v", dir, pkt) })
	}
	bs := chirpstore.NewCAS(peer, nil)
	return newCAS(bs), nil
}

// WithStore calls f with a store opened from the configuration. The store is
// closed after f returns. The error returned by f is returned by WithStore.
func (s *Settings) WithStore(ctx context.Context, f func(CAS) error) error {
	addr, ok := s.FindAddress()
	if !ok {
		return fmt.Errorf("no store service address (%q)", addr)
	}
	return s.WithStoreAddress(ctx, addr, f)
}

// WithStoreAddress calls f with a store opened at addr. The store is closed
// after f returns. The error returned by f is returned by WithStore.
func (s *Settings) WithStoreAddress(ctx context.Context, addr string, f func(CAS) error) error {
	bs, err := s.OpenStoreAddress(ctx, addr)
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
func OpenPath(ctx context.Context, s CAS, path string) (*PathInfo, error) {
	out := &PathInfo{Path: path}

	first, rest := SplitPath(path)

	// Check for a @file key prefix; otherwise it should be a root.
	if !strings.HasPrefix(first, "@") {
		rp, err := root.Open(ctx, s.Roots(), first)
		if err != nil {
			return nil, err
		}
		rf, err := rp.File(ctx, s)
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

	} else if fp, err := file.Open(ctx, s, fk); err != nil {
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

// CAS is a wrapper around a blob.CAS that adds methods to expose the root and
// data buckets.
//
// Tools using this package partition the keyspace into two buckets. The "data"
// bucket comprises all content-addressed keys; the "roots" bucket is for all
// other (non-content-addressed) keys. This is mapped onto the underlying store
// by appending the suffix "." (Unicode 46) to data keys, and "@" (Unicode 64)
// to root keys.
//
// The methods of the CAS access the data keyspace by default; call Roots to
// derive a view of the root keyspace.
type CAS struct {
	affixed.CAS
}

// SyncKeys implements the extension method of blob.SyncKeyer.
func (c CAS) SyncKeys(ctx context.Context, keys []string) ([]string, error) {
	// By construction, the base store will always satisfy this.
	sk := c.CAS.Base().(blob.SyncKeyer)
	wrapped := make([]string, len(keys))
	for i, key := range keys {
		wrapped[i] = c.CAS.WrapKey(key)
	}
	got, err := sk.SyncKeys(ctx, wrapped)
	if err != nil {
		return nil, err
	}
	for i, key := range got {
		got[i] = c.CAS.UnwrapKey(key)
	}
	return got, nil
}

func newCAS(bs blob.CAS) CAS {
	return CAS{CAS: affixed.NewCAS(bs).WithSuffix(dataBucketSuffix)}
}

const (
	dataBucketSuffix = "."
	rootBucketSuffix = "@"
	rootKeyTag       = "\x00\x00"
)

// Roots returns the root view of c.
func (c CAS) Roots() blob.CAS { return CAS{CAS: c.CAS.Derive(rootKeyTag, rootBucketSuffix)} }

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
