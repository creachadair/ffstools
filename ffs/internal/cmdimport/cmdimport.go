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

// Package cmdimport implements the "ffs import" subcommand.
package cmdimport

import (
	"errors"
	"flag"
	"fmt"
	"log"

	"github.com/creachadair/command"
	"github.com/creachadair/ffs/file"
	"github.com/creachadair/ffs/filetree"
	"github.com/creachadair/ffstools/ffs/config"
	"github.com/creachadair/ffstools/lib/editlib"
	"github.com/creachadair/ffstools/lib/importlib"
	"github.com/creachadair/flax"
	"github.com/creachadair/mds/shell"
)

const intoHelp = `

With --into, the resulting file tree is stored under the specified path
of the form <root-key>/<path> or @<file-key>/<path>. In this form, only
one input path is allowed.`

var putConfig importlib.Config

var importFlags struct {
	Target string `flag:"into,Store the resulting object under this root/path or file/path"`
	Edit   string `flag:"edit,Apply this edit to each imported file (see 'file edit')"`
}

var Command = &command.C{
	Name:  "import",
	Usage: "[flags] <local-path> ...\n[flags] <subcommand> ...",
	Help: `Import one or more file trees from archives.

Recursively copy each specified path from the local filesystem to the
store, and print the storage key. By default, file and directory stat
info are recorded; use --nostat to disable this. Use --xattr to capture
extended attributes.

Symbolic links are captured, but devices, sockets, FIFO, and other
special files are skipped.` + intoHelp + `

If --edit is set, the specified edit operations (see "file edit") are
applied to each file tree imported, after the import is complete. The
edit program is a single string, use quotes if calling from a shell.`,

	SetFlags: func(_ *command.Env, fs *flag.FlagSet) {
		fs.BoolVar(&putConfig.OmitStat, "nostat", false, "Omit file and directory stat")
		fs.BoolVar(&putConfig.IncludeXAttr, "xattr", false, "Capture extended attributes")
		fs.BoolVar(&putConfig.Verbose, "v", false, "Enable verbose logging")
		fs.StringVar(&putConfig.FilterName, "filter", ".ffsignore", "Read ignore rules from this file")
		flax.MustBind(fs, &importFlags)
	},
	Run: command.Adapt(runImport),

	Commands: []*command.C{{
		Name:  "tar",
		Usage: "<tar-file-path> ...",
		Help: `Import file trees from Unix tape archive (tar) files.

Paths ending in ".zst" or ".zstd" are automatically decompressed.
Use "-" for the path to read an (uncompressed) archive from stdin.` + intoHelp,
		Run: command.Adapt(runImportTar),
	}, {
		Name:  "zip",
		Usage: "<zip-file-path> ...",
		Help:  `Import file trees from ZIP archive files.` + intoHelp,
		Run:   command.Adapt(runImportZIP),
	}},
}

func runImport(env *command.Env, srcPath string, rest []string) error {
	if importFlags.Target != "" && len(rest) != 0 {
		return env.Usagef("only one path is allowed when --into is set")
	}
	edit, err := parseEdit()
	if err != nil {
		return err
	}

	cfg := env.Config.(*config.Settings)
	return cfg.WithStore(env.Context(), func(s filetree.Store) error {
		if err := checkTarget(env, s, importFlags.Target); err != nil {
			return err
		}
		keys := make([]string, len(env.Args))
		for i, path := range env.Args {
			if putConfig.Verbose {
				log.Printf("begin import: %s", path)
			}
			f, err := putConfig.ImportPath(env.Context(), s.Files(), path)
			if err != nil {
				return err
			}
			if err := edit.ApplyRecursive(env.Context(), f); err != nil {
				return err
			}
			key, err := f.Flush(env.Context())
			if err != nil {
				return err
			}
			keys[i] = key
			if putConfig.Verbose {
				log.Printf("done import: %s (%s)", path, filetree.FormatKey32(key))
			}
		}
		for _, key := range keys {
			fmt.Printf("import: %s\n", filetree.FormatKey32(key))
		}

		if importFlags.Target != "" {
			tf, err := file.Open(env.Context(), s.Files(), keys[0])
			if err != nil {
				return err
			}
			key, err := s.SetPath(env.Context(), importFlags.Target, tf)
			if err != nil {
				return err
			}
			fmt.Printf("set: %s\n", filetree.FormatKey32(key))
		}
		return nil
	})
}

func runImportTar(env *command.Env, srcPath string, rest []string) error {
	if importFlags.Target != "" && len(rest) != 0 {
		return env.Usagef("only one path is allowed when --into is set")
	}
	edit, err := parseEdit()
	if err != nil {
		return err
	}

	cfg := env.Config.(*config.Settings)
	return cfg.WithStore(env.Context(), func(s filetree.Store) error {
		if err := checkTarget(env, s, importFlags.Target); err != nil {
			return err
		}
		var lastRoot *file.File
		for _, path := range env.Args {
			root, err := putConfig.ImportTarPath(env.Context(), s.Files(), path)
			if err != nil {
				return fmt.Errorf("input %q: %w", path, err)
			}
			if err := edit.ApplyRecursive(env.Context(), root); err != nil {
				return fmt.Errorf("edit %q: %w", path, err)
			}
			fmt.Printf("import: %s\n", filetree.FormatKey32(root.Key()))
			lastRoot = root
		}

		// If the --into flag was set, then we know there was exactly one import
		// (because we checked that at the top) and lastRoot is its root (or else
		// we would not have gotten here).
		if importFlags.Target != "" {
			key, err := s.SetPath(env.Context(), importFlags.Target, lastRoot)
			if err != nil {
				return err
			}
			fmt.Printf("set: %s\n", filetree.FormatKey32(key))
		}
		return nil
	})
}

func runImportZIP(env *command.Env, srcPath string, rest []string) error {
	if importFlags.Target != "" && len(rest) != 0 {
		return env.Usagef("only one path is allowed when --into is set")
	}
	edit, err := parseEdit()
	if err != nil {
		return err
	}

	cfg := env.Config.(*config.Settings)
	return cfg.WithStore(env.Context(), func(s filetree.Store) error {
		if err := checkTarget(env, s, importFlags.Target); err != nil {
			return err
		}
		var lastRoot *file.File
		for _, path := range env.Args {
			root, err := putConfig.ImportZIPPath(env.Context(), s.Files(), path)
			if err != nil {
				return err
			}
			if err := edit.ApplyRecursive(env.Context(), root); err != nil {
				return fmt.Errorf("edit %q: %w", path, err)
			}
			fmt.Printf("import: %s\n", filetree.FormatKey32(root.Key()))
			lastRoot = root
		}

		// If the --into flag was set, then we know there was exactly one import
		// (because we checked that at the top) and lastRoot is its root (or else
		// we would not have gotten here).
		if importFlags.Target != "" {
			key, err := s.SetPath(env.Context(), importFlags.Target, lastRoot)
			if err != nil {
				return err
			}
			fmt.Printf("set: %s\n", filetree.FormatKey32(key))
		}
		return nil
	})
}

func checkTarget(env *command.Env, s filetree.Store, target string) error {
	if target != "" {
		root, _ := filetree.SplitPath(target)
		_, err := s.OpenPath(env.Context(), root)
		if err != nil {
			return fmt.Errorf("target %q: %w", target, err)
		}
	}
	return nil
}

func parseEdit() (*editlib.Edit, error) {
	spec, ok := shell.Split(importFlags.Edit)
	if !ok {
		return nil, errors.New("unbalanced quotes in --edit spec")
	} else if len(spec) == 0 {
		return nil, nil // nothing to do
	}
	e, err := editlib.ParseEdit(spec)
	if err != nil {
		return nil, err
	} else if e.DataSpec != nil || e.Create {
		return nil, errors.New("the 'create' and 'data' --edit verbs are not supported")
	}
	return e, nil
}
