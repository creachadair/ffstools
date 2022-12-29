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
	"net"
	"os"
	"path"
	"strings"

	"github.com/creachadair/chirp"
	"github.com/creachadair/chirp/channel"
	"github.com/creachadair/chirpstore"
	"github.com/creachadair/ffs/blob"
	"github.com/creachadair/ffs/file"
	"github.com/creachadair/ffs/file/root"
	"github.com/creachadair/ffs/fpath"
	"github.com/creachadair/ffs/storage/prefixed"
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
	// Context value governing the execution of the tool.
	Context context.Context `json:"-" yaml:"-"`

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
	Tag     string `json:"tag" yaml:"tag"`
	Address string `json:"address" yaml:"address"`
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
func (s *Settings) OpenStore() (blob.CAS, error) {
	addr, ok := s.FindAddress()
	if !ok {
		return nil, fmt.Errorf("no store service address (%q)", addr)
	}
	return s.OpenStoreAddress(s.Context, addr)
}

// OpenStoreAddress connects to the store service at addr.  The caller is
// responsible for closing the store when it is no longer needed.
func (s *Settings) OpenStoreAddress(_ context.Context, addr string) (blob.CAS, error) {
	conn, err := net.Dial(chirp.SplitAddress(addr))
	if err != nil {
		return nil, fmt.Errorf("dialing store: %w", err)
	}
	peer := chirp.NewPeer().Start(channel.IO(conn, conn))
	if s.EnableDebugLogging {
		lg := log.New(log.Writer(), "[ffs] ", log.LstdFlags|log.Lmicroseconds)
		peer.LogPackets(func(pkt chirp.PacketInfo) { lg.Print(pkt) })
	}
	bs := chirpstore.NewCAS(peer, nil)
	return prefixed.NewCAS(bs).Derive(" "), nil
}

// WithStore calls f with a store opened from the configuration. The store is
// closed after f returns. The error returned by f is returned by WithStore.
func (s *Settings) WithStore(ctx context.Context, f func(blob.CAS) error) error {
	addr, ok := s.FindAddress()
	if !ok {
		return fmt.Errorf("no store service address (%q)", addr)
	}
	return s.WithStoreAddress(ctx, addr, f)
}

// WithStoreAddress calls f with a store opened at addr. The store is closed
// after f returns. The error returned by f is returned by WithStore.
func (s *Settings) WithStoreAddress(ctx context.Context, addr string, f func(blob.CAS) error) error {
	bs, err := s.OpenStoreAddress(ctx, addr)
	if err != nil {
		return err
	}
	defer blob.CloseStore(ctx, bs)
	return f(bs)
}

// Roots derives a view of roots from bs.
func Roots(bs blob.CAS) prefixed.CAS { return prefixed.NewCAS(bs).Derive("@") }

// ParseKey parses the string encoding of a key.  By default, s must be hex
// encoded. If s begins with "@", it is taken literally. If s begins with "+"
// it is taken as base64.
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
func PrintableKey(key string) interface{} {
	for i := 0; i < len(key); i++ {
		if key[i] < ' ' || key[i] > '~' {
			return base64.StdEncoding.EncodeToString([]byte(key))
		}
	}
	return key
}

// ToJSON converts a value to indented JSON.
func ToJSON(msg interface{}) string {
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
func OpenPath(ctx context.Context, s blob.CAS, path string) (*PathInfo, error) {
	out := &PathInfo{Path: path}

	first, rest := SplitPath(path)

	// Check for a @file key prefix; otherwise it should be a root.
	if !strings.HasPrefix(first, "@") {
		rp, err := root.Open(ctx, Roots(s), first)
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
	out.FileKey, _ = out.File.Flush(ctx) // safe, it was just opened
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
