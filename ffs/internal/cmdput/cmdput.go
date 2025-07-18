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

// Package cmdput implements the "ffs put" subcommand.
package cmdput

import (
	"flag"
	"fmt"
	"log"

	"github.com/creachadair/command"
	"github.com/creachadair/ffs/file"
	"github.com/creachadair/ffs/filetree"
	"github.com/creachadair/ffstools/ffs/config"
	"github.com/creachadair/ffstools/lib/putlib"
	"github.com/creachadair/flax"
)

var putConfig putlib.Config

var putFlags struct {
	Target string `flag:"into,Store the resulting object under this root/path or file/path"`
}

var Command = &command.C{
	Name:  "put",
	Usage: "<path> ...",
	Help: `Write file and directory contents to the store.

Recursively copy each specified path from the local filesystem to the
store, and print the storage key. By default, file and directory stat
info are recorded; use --nostat to disable this. Use --xattr to capture
extended attributes.

Symbolic links are captured, but devices, sockets, FIFO, and other
special files are skipped.

With "--into", the resulting file is stored under the specified path of
the form <root-key>/<path> or @<file-key>/<path>. In this form, only one
input path is allowed.`,

	SetFlags: func(_ *command.Env, fs *flag.FlagSet) {
		fs.BoolVar(&putConfig.NoStat, "nostat", false, "Omit file and directory stat")
		fs.BoolVar(&putConfig.XAttr, "xattr", false, "Capture extended attributes")
		fs.BoolVar(&putConfig.Verbose, "v", false, "Enable verbose logging")
		fs.StringVar(&putConfig.FilterName, "filter", ".ffsignore", "Read ignore rules from this file")
		flax.MustBind(fs, &putFlags)
	},
	Run: command.Adapt(runPut),
}

func runPut(env *command.Env, srcPath string, rest []string) error {
	if putFlags.Target != "" && len(rest) != 0 {
		return env.Usagef("only one path is allowed when -target is set")
	}

	cfg := env.Config.(*config.Settings)
	return cfg.WithStore(env.Context(), func(s filetree.Store) error {
		if err := checkTarget(env, s, putFlags.Target); err != nil {
			return err
		}
		keys := make([]string, len(env.Args))
		for i, path := range env.Args {
			if putConfig.Verbose {
				log.Printf("begin put: %s", path)
			}
			f, err := putConfig.PutPath(env.Context(), s.Files(), path)
			if err != nil {
				return err
			}
			key, err := f.Flush(env.Context())
			if err != nil {
				return err
			}
			keys[i] = key
			if putConfig.Verbose {
				log.Printf("done put: %s (%s)", path, config.FormatKey(key))
			}
		}
		for _, key := range keys {
			fmt.Printf("put: %s\n", config.FormatKey(key))
		}

		if putFlags.Target != "" {
			tf, err := file.Open(env.Context(), s.Files(), keys[0])
			if err != nil {
				return err
			}
			key, err := putlib.SetPath(env.Context(), s, putFlags.Target, tf)
			if err != nil {
				return err
			}
			fmt.Printf("set: %s\n", config.FormatKey(key))
		}
		return nil
	})
}

func checkTarget(env *command.Env, s filetree.Store, target string) error {
	if target != "" {
		root, _ := filetree.SplitPath(target)
		_, err := filetree.OpenPath(env.Context(), s, root)
		if err != nil {
			return fmt.Errorf("target %q: %w", target, err)
		}
	}
	return nil
}
