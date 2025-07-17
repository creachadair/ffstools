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
	"encoding/json"
	"fmt"
	"log"
	"net"
	"os"
	"strings"
	"time"

	"github.com/creachadair/chirp"
	"github.com/creachadair/chirp/channel"
	"github.com/creachadair/chirpstore"
	"github.com/creachadair/ffs/blob"
	"github.com/creachadair/ffs/file/wiretype"
	"github.com/creachadair/ffs/filetree"
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

	// Timeout for dialing store connections.
	DialTimeout Duration `json:"dialTimeout" yaml:"dial-timeout"`

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
		sub := s.Substore
		if i := strings.LastIndex(tag, "+"); i >= 0 {
			tag, sub = tag[:i], tag[i+1:]
		}
		for _, st := range s.Stores {
			if tag == st.Tag {
				cp := *st
				ExpandString(&cp.Spec)
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
	}
	return StoreSpec{Spec: spec, Prefix: s.ServicePrefix, Substore: s.Substore}
}

// OpenStore connects to the store service address in the configuration.  The
// caller is responsible for closing the store when it is no longer needed.
func (s *Settings) OpenStore(ctx context.Context) (filetree.Store, error) {
	spec := s.ResolveAddress(s.DefaultStore)
	if spec.Address == "" {
		return filetree.Store{}, fmt.Errorf("no store service address (%q)", s.DefaultStore)
	}
	return s.openStoreAddress(ctx, spec)
}

// openStoreAddress connects to the store service at addr.  The caller is
// responsible for closing the store when it is no longer needed.
func (s *Settings) openStoreAddress(ctx context.Context, spec StoreSpec) (filetree.Store, error) {
	lg := log.New(log.Writer(), "[ffs] ", log.LstdFlags|log.Lmicroseconds)
	if s.EnableDebugLogging {
		lg.Printf("dial %q", spec.Address)
	}
	var d net.Dialer
	if s.DialTimeout > 0 {
		d.Timeout = time.Duration(s.DialTimeout)
	}
	conn, err := d.Dial(chirp.SplitAddress(spec.Address))
	if err != nil {
		return filetree.Store{}, fmt.Errorf("dialing store: %w", err)
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
			return filetree.Store{}, fmt.Errorf("open substore %q: %w", s.Substore, err)
		}
	}
	return filetree.NewStore(ctx, sub)
}

// WithStore calls f with a store opened from the configuration. The store is
// closed after f returns. The error returned by f is returned by WithStore.
func (s *Settings) WithStore(ctx context.Context, f func(filetree.Store) error) error {
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
func (s *Settings) WithStoreAddress(ctx context.Context, addr string, f func(filetree.Store) error) error {
	spec := s.ResolveAddress(addr)
	bs, err := s.openStoreAddress(ctx, spec)
	if err != nil {
		return err
	}
	defer bs.Close(ctx)
	return f(bs)
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

// Duration is a wrapper around [time.Duration] that encodes as a string in JSON.
type Duration time.Duration

func (t Duration) Duration() time.Duration { return time.Duration(t) }

func (t Duration) MarshalText() ([]byte, error) {
	return []byte(time.Duration(t).String()), nil
}

func (t *Duration) UnmarshalText(text []byte) error {
	d, err := time.ParseDuration(string(text))
	if err != nil {
		return err
	}
	*t = Duration(d)
	return nil
}
