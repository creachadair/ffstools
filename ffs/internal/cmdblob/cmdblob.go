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

// Package cmdblob provides basic support for reading and writing
// implementations of the blob.Store interface.
package cmdblob

import (
	"flag"

	"github.com/creachadair/command"
	"github.com/creachadair/flax"
)

var blobFlags struct {
	KV        string `flag:"kv,Filter keys to this keyspace label"`              // global
	Replace   bool   `flag:"replace,Replace an existing key"`                    // put
	MissingOK bool   `flag:"missing-ok,Do not report an error for missing keys"` // delete
}

var listFlags struct {
	Raw     bool   `flag:"raw,Print raw keys without hex encoding"`
	Start   string `flag:"start,List keys greater than or equal to this"`
	Prefix  string `flag:"prefix,List only keys having this prefix"`
	MaxKeys int    `flag:"max,List at most this many keys (0=all)"`
}

var bf = flax.MustCheck(&blobFlags)

var Command = &command.C{
	Name: "blob",
	Help: `Manipulate the contents of a blob store.

Since blob keys are usually binary, key arguments are assumed to be encoded.

Rule                                                     Example
- To specify blob keys literally, prefix them with "@"   @foo
  To escape a leading @, double it                       @@foo
- If the key is all hex digits, decode it as hex         666f6f0a
- Otherwise, it is treated as base64.                    Zm9vCg==

If the FFS_STORE environment variable is set, it is read to set the
address of the storage server. Otherwise, --store must be set to either
the address or an @tag from the configuration file.
`,

	SetFlags: func(env *command.Env, fs *flag.FlagSet) { bf.Flag("kv").Bind(fs) },

	Commands: []*command.C{
		{
			Name:  "get",
			Usage: "<key>...",
			Help:  "Read blobs from the store.",
			Run:   getCmd,
		},
		{
			Name:  "put",
			Usage: "<key> [<path>]",
			Help:  "Write a blob to the store.",

			SetFlags: func(env *command.Env, fs *flag.FlagSet) { bf.Flag("replace").Bind(fs) },
			Run:      command.Adapt(putCmd),
		},
		{
			Name:  "cas-put",
			Usage: "[<path>]",
			Help:  "Write a content-addressed blob to the store.",
			Run:   command.Adapt(casPutCmd),
		},
		{
			Name:  "has",
			Usage: "<key>...",
			Help:  "Report which of the specified blobs are present in the store.",
			Run:   hasCmd,
		},
		{
			Name:  "delete",
			Usage: "<key> ...",
			Help:  "Delete blobs from the store.",

			SetFlags: func(env *command.Env, fs *flag.FlagSet) { bf.Flag("missing-ok").Bind(fs) },
			Run:      delCmd,
		},
		{
			Name: "list",
			Help: "List keys in the store.",

			SetFlags: command.Flags(flax.MustBind, &listFlags),
			Run:      command.Adapt(listCmd),
		},
		{
			Name: "len",
			Help: "Print the number of stored keys.",
			Run:  command.Adapt(lenCmd),
		},
		{
			Name:     "copy",
			Usage:    "<src> <dst>",
			Help:     "Copy the contents of one blob to another key.",
			SetFlags: func(env *command.Env, fs *flag.FlagSet) { bf.Flag("replace").Bind(fs) },
			Run:      command.Adapt(copyCmd),
		},
		{
			Name:  "sync-keys",
			Usage: "<key>...",
			Help:  "Report which of the specified keys are not in the store.",
			Run:   command.Adapt(syncKeysCmd),
		},
	},
}
