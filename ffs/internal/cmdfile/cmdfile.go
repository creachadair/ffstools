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

package cmdfile

import (
	"bufio"
	"fmt"
	"io"
	"io/fs"
	"os"

	"github.com/creachadair/command"
	"github.com/creachadair/ffs/blob"
	"github.com/creachadair/ffs/file"
	"github.com/creachadair/ffs/file/wiretype"
	"github.com/creachadair/ffs/fpath"
	"github.com/creachadair/ffstools/ffs/config"
)

const fileCmdUsage = `@<root-key>[/path] ...
<file-key>[/path] ...`

var Command = &command.C{
	Name: "file",
	Help: `Manipulate file and directory objects

File objects are addressed by storage keys. The storage key for
a file may be specified in the following formats:

  @<root-name>                 : the file key from a root pointer
  74686973206973206d79206b6579 : hexadecimal encoded
  dGhpcyBpcyBteSBrZXk=         : base64 encoded
`,

	Commands: []*command.C{
		{
			Name:  "show",
			Usage: fileCmdUsage,
			Help:  "Print the representation of a file object",

			Run: runShow,
		},
		{
			Name:  "read",
			Usage: fileCmdUsage,
			Help:  "Read the binary contents of a file object",

			Run: runRead,
		},
		{
			Name: "set",
			Usage: `@<root-key>/<path> <target-key>
<origin-key>/<path> <file-key>`,
			Help: `Set the specified path beneath the origin to the given target

The storage key of the modified origin is printed to stdout.
If the origin is from a root, the root is updated with the modified origin.
`,

			Run: runSet,
		},
		{
			Name: "remove",
			Usage: `@<root-key>/<path> ...
<origin-key>/<path> ...`,
			Help: `Remove the specified path from beneath the origin

The storage key of the modified origin is printed to stdout.
If the origin is from a root, the root is updated with the modified origin.
`,

			Run: runRemove,
		},
	},
}

func runShow(env *command.Env, args []string) error {
	if len(args) == 0 {
		return env.Usagef("missing required origin/path")
	}
	cfg := env.Config.(*config.Settings)
	return cfg.WithStore(cfg.Context, func(s blob.CAS) error {
		for _, arg := range args {
			if arg == "" {
				return env.Usagef("origin may not be empty")
			}
			of, err := config.OpenPath(cfg.Context, s, arg)
			if err != nil {
				return err
			}

			msg := file.Encode(of.File).Value.(*wiretype.Object_Node).Node
			fmt.Println(config.ToJSON(map[string]interface{}{
				"storageKey": []byte(of.FileKey),
				"node":       msg,
			}))
		}
		return nil
	})
}

func runRead(env *command.Env, args []string) error {
	if len(args) == 0 {
		return env.Usagef("missing required origin/path")
	}
	cfg := env.Config.(*config.Settings)
	return cfg.WithStore(cfg.Context, func(s blob.CAS) error {
		of, err := config.OpenPath(cfg.Context, s, args[0])
		if err != nil {
			return err
		}
		r := bufio.NewReaderSize(of.File.Cursor(cfg.Context), 1<<20)
		_, err = io.Copy(os.Stdout, r)
		return err
	})
}

func runSet(env *command.Env, args []string) error {
	if len(args) != 2 {
		return env.Usagef("got %d arguments, wanted origin/path, target", len(args))
	}
	obase, orest := config.SplitPath(args[0])
	if orest == "" {
		return env.Usagef("path must not be empty")
	}
	targetKey, err := config.ParseKey(args[1])
	if err != nil {
		return fmt.Errorf("target key: %w", err)
	}

	cfg := env.Config.(*config.Settings)
	return cfg.WithStore(cfg.Context, func(s blob.CAS) error {
		tf, err := file.Open(cfg.Context, s, targetKey)
		if err != nil {
			return fmt.Errorf("target file: %w", err)
		}
		of, err := config.OpenPath(cfg.Context, s, obase) // N.B. No path; see below
		if err != nil {
			return err
		}

		if _, err := fpath.Set(cfg.Context, of.Base, orest, &fpath.SetOptions{
			Create: true,
			SetStat: func(st *file.Stat) {
				if st.Mode == 0 {
					st.Mode = fs.ModeDir | 0755
				}
			},
			File: tf,
		}); err != nil {
			return err
		}
		key, err := of.Flush(cfg.Context)
		if err != nil {
			return err
		}
		fmt.Printf("%x\n", key)
		return nil
	})
}

func runRemove(env *command.Env, args []string) error {
	if len(args) == 0 {
		return env.Usagef("missing origin/path")
	}

	cfg := env.Config.(*config.Settings)
	return cfg.WithStore(cfg.Context, func(s blob.CAS) error {
		for _, arg := range args {
			base, rest := config.SplitPath(arg)
			if rest == "" {
				return fmt.Errorf("missing path %q", arg)
			}
			of, err := config.OpenPath(cfg.Context, s, base) // N.B. No path; see below
			if err != nil {
				return err
			}

			if err := fpath.Remove(cfg.Context, of.Base, rest); err != nil {
				return err
			}
			key, err := of.Flush(cfg.Context)
			if err != nil {
				return err
			}
			fmt.Printf("%x\n", key)
		}
		return nil
	})
}
