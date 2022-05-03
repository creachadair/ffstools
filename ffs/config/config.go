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
	"net"
	"os"
	"strings"

	"github.com/creachadair/ffs/blob"
	"github.com/creachadair/ffs/storage/prefixed"
	"github.com/creachadair/jrpc2"
	"github.com/creachadair/jrpc2/channel"
	"github.com/creachadair/rpcstore"
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
	return OpenStore(s.Context, addr)
}

// OpenStore connects to the store service at addr.  The caller is responsible
// for closing the store when it is no longer needed.
func OpenStore(_ context.Context, addr string) (blob.CAS, error) {
	conn, err := net.Dial(jrpc2.Network(addr))
	if err != nil {
		return nil, fmt.Errorf("dialing store: %w", err)
	}
	ch := channel.Line(conn, conn)
	bs := rpcstore.NewCAS(jrpc2.NewClient(ch, nil), nil)
	return prefixed.NewCAS(bs).Derive(" "), nil
}

// WithStore calls f with a store opened from the configuration. The store is
// closed after f returns. The error returned by f is returned by WithStore.
func (s *Settings) WithStore(ctx context.Context, f func(blob.CAS) error) error {
	addr, ok := s.FindAddress()
	if !ok {
		return fmt.Errorf("no store service address (%q)", addr)
	}
	return WithStore(ctx, addr, f)
}

// WithStore calls f with a store opened at addr. The store is closed after f
// returns. The error returned by f is returned by WithStore.
func WithStore(ctx context.Context, addr string, f func(blob.CAS) error) error {
	bs, err := OpenStore(ctx, addr)
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
