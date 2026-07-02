// Copyright 2022 Michael J. Fromberger. All Rights Reserved.
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

// Package cmdexport implements the "ffs export" subcommand.
package cmdexport

import (
	"os"
	"path/filepath"

	"github.com/creachadair/command"
	"github.com/creachadair/ffs/filetree"
	"github.com/creachadair/ffstools/ffs/config"
	"github.com/creachadair/ffstools/lib/exportlib"
	"github.com/creachadair/flax"
)

var exportFlags struct {
	NoStat  bool   `flag:"nostat,Do not update permissions or modification times"`
	XAttr   bool   `flag:"xattr,Restore extended attributes"`
	Verbose bool   `flag:"v,Enable verbose logging"`
	Target  string `flag:"to,Export to this path (required)"`
	Update  bool   `flag:"update,Update target path if it exists"`
}

var Command = &command.C{
	Name:  "export",
	Usage: "<root-key>[/path/...]\n@<file-key>[/path/...]",
	Help: `
Export a file tree to the local filesystem.

Recursively export the file indicated by the selected root or file storage
key to the path indicated by --to. By default, stat information (permissions,
modification time, etc.) is copied to the output; use --nostat to omit this.
Use --xattr to export extended attributes, if any are stored.`,

	SetFlags: command.Flags(flax.MustBind, &exportFlags),
	Run:      command.Adapt(runExport),

	Commands: []*command.C{{
		Name:  "tar",
		Usage: "<root-key>[/path/...] ...\n@<file-key>[/path/...] ...",
		Help: `
Export file trees to a tar archive.

Recursively export the files indicated by the selected root or file storage keys to
a tar stream. If --to is set, the output is written to that file, which is created
if necessary; otherwise the output is written to stdout. Unless --update is set,
the specified file name must not already exist.

If --compress is true, or if the --to filename ends in ".zst" or ".zstd", the output
is compressed with zstd.`,

		SetFlags: command.Flags(flax.MustBind, &tarFlags),
		Run:      command.Adapt(runTarExport),
	}, {
		Name: "zip",
		Usage: `--to <zipfile> <root-key>[/path/...] ...
--to <zipfile> @<file-key>[/path/...] ...`,
		Help: `
Export file trees to a ZIP archive.

Recursively export the file indicated by the selected root or file storage key
to a ZIP archive in the specified file, which is created if necessary.
Unless --update is set, the specified ZIP file name must not already exist.`,
		SetFlags: command.Flags(flax.MustBind, &zipFlags),
		Run:      command.Adapt(runZipExport),
	}},
}

func runExport(env *command.Env, originPath string) error {
	if exportFlags.Target == "" {
		return env.Usagef("missing required --to path")
	}

	// Create leading components of the target directory path, as required.
	if err := os.MkdirAll(filepath.Dir(exportFlags.Target), 0700); err != nil {
		return err
	}

	cfg := env.Config.(*config.Settings)
	return cfg.WithStore(env.Context(), func(s filetree.Store) error {
		of, err := s.OpenPath(env.Context(), originPath)
		if err != nil {
			return err
		}
		ec := exportConfig(env, "") // root is not used here
		return ec.FileToOS(env.Context(), of, exportFlags.Target)
	})
}

func exportConfig(env *command.Env, root string) exportlib.Config {
	ec := exportlib.Config{
		Root:         root,
		IncludeXAttr: exportFlags.XAttr,
		OmitStat:     exportFlags.NoStat,
		Update:       exportFlags.Update,
	}
	if exportFlags.Verbose {
		ec.DebugOutput = env
	}
	return ec
}

func openFlags() int {
	const base = os.O_RDWR | os.O_TRUNC | os.O_CREATE
	if exportFlags.Update {
		return base
	}
	return base | os.O_EXCL
}
