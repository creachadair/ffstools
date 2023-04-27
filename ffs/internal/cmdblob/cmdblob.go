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
	// Flag targets
	Bucket    string `flag:"bucket,Filter keys to this bucket label"`            // global
	Replace   bool   `flag:"replace,Replace an existing key"`                    // put
	Raw       bool   `flag:"raw,Print raw keys without hex encoding"`            // list
	Start     string `flag:"start,List keys greater than or equal to this"`      // list
	Prefix    string `flag:"prefix,List only keys having this prefix"`           // list
	MaxKeys   int    `flag:"max,List at most this many keys (0=all)"`            // list
	MissingOK bool   `flag:"missing-ok,Do not report an error for missing keys"` // delete
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

If the BLOB_STORE environment variable is set, it is read to set the
address of the storage server. Otherwise, -store must be set to either
the address or an @tag from the configuration file.
`,

	SetFlags: func(env *command.Env, fs *flag.FlagSet) { bf.Flag("bucket").Bind(fs) },

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

			SetFlags: func(env *command.Env, fs *flag.FlagSet) { bf.Flag("replace").Bind(fs) },
			Run:      putCmd,
		},
		{
			Name:  "size",
			Usage: "<key>...",
			Help:  "Print the sizes of stored blobs",
			Run:   sizeCmd,
		},
		{
			Name:  "delete",
			Usage: "<key>",
			Help:  "Delete a blob from the store",

			SetFlags: func(env *command.Env, fs *flag.FlagSet) { bf.Flag("missing-ok").Bind(fs) },
			Run:      delCmd,
		},
		{
			Name: "list",
			Help: "List keys in the store",

			SetFlags: func(env *command.Env, fs *flag.FlagSet) {
				bf.Flag("raw").Bind(fs)
				bf.Flag("start").Bind(fs)
				bf.Flag("prefix").Bind(fs)
				bf.Flag("max").Bind(fs)
			},
			Run: listCmd,
		},
		{
			Name: "len",
			Help: "Print the number of stored keys",
			Run:  lenCmd,
		},
		{
			Name: "cas-key",
			Help: "Compute the key for a blob without writing it",
			Run:  casKeyCmd,
		},
		{
			Name:  "cas-put",
			Usage: "cas-put",
			Help:  "Write a content-addressed blob to the store from stdin",
			Run:   casPutCmd,
		},
		{
			Name:     "copy",
			Usage:    "<src> <dst>",
			Help:     "Copy the contents of one blob to another key",
			SetFlags: func(env *command.Env, fs *flag.FlagSet) { bf.Flag("replace").Bind(fs) },
			Run:      copyCmd,
		},
	},
}
