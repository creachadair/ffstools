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
	"archive/tar"
	"archive/zip"
	"bufio"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"

	"github.com/creachadair/command"
	"github.com/creachadair/ffs/filetree"
	"github.com/creachadair/ffstools/ffs/config"
	"github.com/creachadair/ffstools/lib/exportlib"
	"github.com/creachadair/flax"
	"github.com/klauspost/compress/zstd"
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
		ec := exportConfig(env, exportFlags.Target)
		return ec.ExportToOS(env.Context(), of)
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

var zipFlags struct {
	Root string `flag:"root,Prefix all output paths with this directory name"`
}

func runZipExport(env *command.Env, originPaths ...string) (retErr error) {
	if exportFlags.Target == "" {
		return env.Usagef("missing required --to path")
	}
	f, err := os.OpenFile(exportFlags.Target, openFlags(), 0600)
	if err != nil {
		return fmt.Errorf("output: %w", err)
	}
	defer f.Close()
	zw := zip.NewWriter(f)

	cfg := env.Config.(*config.Settings)
	return cfg.WithStore(env.Context(), func(s filetree.Store) error {
		ec := exportConfig(env, zipFlags.Root)
		for _, originPath := range originPaths {
			of, err := s.OpenPath(env.Context(), originPath)
			if err != nil {
				return err
			}
			if werr := ec.ExportToZIP(env.Context(), of, zw); werr != nil {
				zw.Close()
				return fmt.Errorf("copy to archive: %w", err)
			}
		}
		if err := errors.Join(zw.Close(), f.Close()); err != nil {
			return fmt.Errorf("finalize archive: %w", err)
		}
		return nil
	})
}

var tarFlags struct {
	Compress bool   `flag:"compress,Compress output with zstd"`
	Root     string `flag:"root,Prefix all output paths with this directory name"`
}

func runTarExport(env *command.Env, originPath string, rest ...string) (retErr error) {
	var mc mcloser
	defer func() {
		err := mc.Close()
		if retErr == nil {
			retErr = err
		}
	}()

	// Open the output file, either stdout or the named file.
	var w io.Writer
	if exportFlags.Target == "" {
		w = os.Stdout
	} else if f, err := os.OpenFile(exportFlags.Target, openFlags(), 0700); err != nil {
		return fmt.Errorf("output: %w", err)
	} else {
		mc = append(mc, f.Close)
		w = f
	}

	// Stack an output buffer.
	buf := bufio.NewWriter(w)
	mc = append(mc, buf.Flush)
	w = buf

	// If enabled, stack a compressor.
	ext := filepath.Ext(exportFlags.Target)
	if tarFlags.Compress || ext == ".zst" || ext == ".zstd" {
		enc, err := zstd.NewWriter(w)
		if err != nil {
			panic(fmt.Sprintf("zstd writer: %v", err)) // should not be possible
		}
		mc = append(mc, enc.Close)
		w = enc
	}
	tw := tar.NewWriter(w)
	mc = append(mc, tw.Close)

	cfg := env.Config.(*config.Settings)
	return cfg.WithStore(env.Context(), func(s filetree.Store) error {
		ec := exportConfig(env, tarFlags.Root)
		for _, originPath := range env.Args {
			of, err := s.OpenPath(env.Context(), originPath)
			if err != nil {
				return err
			}
			if err := ec.ExportToTar(env.Context(), of, tw); err != nil {
				return fmt.Errorf("export %q: %w", originPath, err)
			}
		}
		return nil
	})
}

type mcloser []func() error

func (m mcloser) Close() error {
	var errs []error
	for _, close := range slices.Backward(m) {
		if err := close(); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}
