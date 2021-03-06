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

// Program blob provides basic support for reading and writing implementations
// of the blob.Store interface.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"path/filepath"

	"github.com/creachadair/command"
	"github.com/creachadair/ffstools/ffs/config"
)

type settings struct {
	Context context.Context
	FFS     *config.Settings

	// Flag targets
	Store     string // global
	Bucket    string // global
	Mode      string // global
	Debug     bool   // global
	Replace   bool   // put
	Raw       bool   // list
	Start     string // list
	Prefix    string // list
	MissingOK bool   // delete
}

func main() {
	command.RunOrFail(tool.NewEnv(&settings{
		Context: context.Background(),
	}), os.Args[1:])
}

var tool = &command.C{
	Name: filepath.Base(os.Args[0]),
	Usage: `[options] command [args...]
help [command]`,
	Help: `Manipulate the contents of a blob store.

Since blob keys are usually binary, key arguments are assumed to be encoded.

Rule                                                     Example
- To specify blob keys literally, prefix them with "@"   @foo
  To escape a leading @, double it                       @@foo
- If the key is all hex digits, decode it as hex         666f6f0a
- Otherwise, it is treated as base64.                    Zm9vCg==

If the BLOB_STORE environment variable is set, it is read to set the
address of the storage server. Otherwise, -store must be set to either
the address or an @tag from the configuration file.

`,

	SetFlags: func(env *command.Env, fs *flag.FlagSet) {
		cfg := env.Config.(*settings)
		fs.StringVar(&cfg.Store, "store", "", "Blob store address (required)")
		fs.StringVar(&cfg.Bucket, "bucket", "", "Prefix to add to all keys")
		fs.StringVar(&cfg.Mode, "mode", "jrpc2", "Service mode (jrpc2 or chirp")
		fs.BoolVar(&cfg.Debug, "debug", false, "Enable client debug logging")
	},

	Init: func(env *command.Env) error {
		fc, err := config.Load(config.Path())
		if err != nil {
			return fmt.Errorf("loading FFS config: %w", err)
		}
		cfg := env.Config.(*settings)
		if cfg.Store != "" {
			fc.DefaultStore = cfg.Store
		} else if bs := os.Getenv("BLOB_STORE"); bs != "" {
			fc.DefaultStore = bs
		}
		fc.Context = cfg.Context
		cfg.FFS = fc
		return nil
	},

	Commands: []*command.C{
		{
			Name:  "get",
			Usage: "get <key>...",
			Help:  "Read blobs from the store",
			Run:   getCmd,
		},
		{
			Name:  "put",
			Usage: "put <key> [<path>]",
			Help:  "Write a blob to the store",

			SetFlags: func(env *command.Env, fs *flag.FlagSet) {
				cfg := env.Config.(*settings)
				fs.BoolVar(&cfg.Replace, "replace", false, "Replace an existing key")
			},
			Run: putCmd,
		},
		{
			Name:  "size",
			Usage: "size <key>...",
			Help:  "Print the sizes of stored blobs",
			Run:   sizeCmd,
		},
		{
			Name:  "delete",
			Usage: "delete <key>",
			Help:  "Delete a blob from the store",

			SetFlags: func(env *command.Env, fs *flag.FlagSet) {
				cfg := env.Config.(*settings)
				fs.BoolVar(&cfg.MissingOK, "missing-ok", false, "Do not report an error for missing keys")
			},
			Run: delCmd,
		},
		{
			Name: "list",
			Help: "List keys in the store",

			SetFlags: func(env *command.Env, fs *flag.FlagSet) {
				cfg := env.Config.(*settings)
				fs.BoolVar(&cfg.Raw, "raw", false, "Print raw keys without hex encoding")
				fs.StringVar(&cfg.Start, "start", "", "List keys greater than or equal to this")
				fs.StringVar(&cfg.Prefix, "prefix", "", "List only keys having this prefix")
			},
			Run: listCmd,
		},
		{
			Name: "len",
			Help: "Print the number of stored keys",
			Run:  lenCmd,
		},
		{
			Name: "cas",
			Help: "Manipulate a content-addressable blob store",

			Commands: []*command.C{
				{
					Name: "key",
					Help: "Compute the key for a blob without writing it",
					Run:  casKeyCmd,
				},
				{
					Name:  "put",
					Usage: "put",
					Help:  "Write a content-addressed blob to the store from stdin.",
					Run:   casPutCmd,
				},
			},
		},
		{
			Name: "copy",
			Help: "Copy the contents of one blob to another key",
			SetFlags: func(env *command.Env, fs *flag.FlagSet) {
				cfg := env.Config.(*settings)
				fs.BoolVar(&cfg.Replace, "replace", false, "Replace an existing key")
			},
			Run: copyCmd,
		},
		{
			Name: "status",
			Help: "Print blob server status",
			Run:  statCmd,
		},
		command.HelpCommand(nil),
	},
}
