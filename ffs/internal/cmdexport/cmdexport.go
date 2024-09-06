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
	"bufio"
	"context"
	"fmt"
	"io"
	"io/fs"
	"log"
	"os"
	"path/filepath"
	"time"

	"github.com/creachadair/atomicfile"
	"github.com/creachadair/command"
	"github.com/creachadair/ffs/file"
	"github.com/creachadair/ffs/fpath"
	"github.com/creachadair/ffstools/ffs/config"
	"github.com/creachadair/flax"
	"github.com/creachadair/taskgroup"
	"github.com/pkg/xattr"
)

var exportFlags struct {
	NoStat  bool   `flag:"nostat,Do not update permissions or modification times"`
	XAttr   bool   `flag:"xattr,Restore extended attributes"`
	Verbose bool   `flag:"v,Enable verbose logging"`
	Target  string `flag:"to,Export to this path (required)"`
	Update  bool   `flag:"update,Update target path if it exists"`
}

var Command = &command.C{
	Name: "export",
	Usage: `<root-key>[/path/...]
@<file-key>[/path/...]`,
	Help: `
Export a file tree to the local filesystem.

Recursively export the file indicated by the selected root or file storage
key to the path indicated by -to. By default, stat information (permissions,
modification time, etc.) is copied to the output; use -nostat to omit this.
Use -xattr to export extended attributes, if any are stored.`,

	SetFlags: command.Flags(flax.MustBind, &exportFlags),
	Run:      command.Adapt(runExport),
}

func runExport(env *command.Env, originPath string) error {
	if exportFlags.Target == "" {
		return env.Usagef("missing required -to path")
	}

	// Create leading components of the target directory path, as required.
	if err := os.MkdirAll(filepath.Dir(exportFlags.Target), 0700); err != nil {
		return err
	}

	cfg := env.Config.(*config.Settings)
	return cfg.WithStore(env.Context(), func(s config.CAS) error {
		of, err := config.OpenPath(env.Context(), s, originPath)
		if err != nil {
			return err
		}
		cctx, cancel := context.WithCancel(env.Context())
		defer cancel()
		g, start := taskgroup.New(taskgroup.Trigger(cancel)).Limit(64)

		g.Go(func() error {
			return fpath.Walk(cctx, of.File, func(e fpath.Entry) error {
				if err := cctx.Err(); err != nil {
					return err
				}

				opath := filepath.Join(exportFlags.Target, filepath.FromSlash(e.Path))
				if !e.File.Stat().Mode.IsDir() {
					start(func() error {
						return exportFile(cctx, e.File, opath)
					})
					return nil
				}
				return exportFile(cctx, e.File, opath)
			})
		})
		return g.Wait()
	})
}

func exportFile(ctx context.Context, f *file.File, path string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	mode := f.Stat().Mode
	var link bool
	if mode.IsDir() {
		logPrintf("create directory %q", path)
		if err := os.Mkdir(path, 0700); err != nil {
			if !exportFlags.Update || !os.IsExist(err) {
				return err
			}
		}
	} else if mode.Type()&fs.ModeSymlink != 0 {
		logPrintf("write symlink %q", path)
		if err := linkFile(ctx, f, path); err != nil {
			return err
		}
		link = true
	} else {
		if !exportFlags.Update {
			_, err := os.Lstat(path)
			if err == nil {
				return fmt.Errorf("file %q exists", path)
			}
		}
		nw, err := copyFile(ctx, f, path)
		if err != nil {
			return err
		}
		logPrintf("write file %q (%d bytes)", path, nw)
	}

	// Restore permissions and modification times, if requested and available.
	if !exportFlags.NoStat && f.Stat().Persistent() && !link {
		stat := f.Stat()
		logPrintf("- set mode %v, mtime %v",
			stat.Mode.Perm(), stat.ModTime.Format(time.RFC3339))

		if err := os.Chmod(path, stat.Mode); err != nil {
			return fmt.Errorf("setting permissions: %w", err)
		}
		if err := os.Chtimes(path, stat.ModTime, stat.ModTime); err != nil {
			return fmt.Errorf("setting modtime: %w", err)
		}

		// TODO(creachadair): Maybe set owner/group?
	}

	// Restore extended attributes if requested.
	if exportFlags.XAttr {
		xa := f.XAttr()
		for _, key := range xa.Names() {
			val := xa.Get(key)
			logPrintf("- set xattr %q (%d bytes)", key, len(val))
			if xerr := xattr.LSet(path, key, []byte(val)); xerr != nil {
				return fmt.Errorf("setting xattrs %q: %w", key, xerr)
			}
		}
	}
	return nil
}

func copyFile(ctx context.Context, f *file.File, path string) (int64, error) {
	r := bufio.NewReaderSize(f.Cursor(ctx), 1<<20)
	return atomicfile.WriteAll(path, r, 0600)
}

func linkFile(ctx context.Context, f *file.File, path string) error {
	target, err := io.ReadAll(f.Cursor(ctx))
	if err != nil {
		return fmt.Errorf("reading link target: %w", err)
	}
	return os.Symlink(string(target), path)
}

func logPrintf(msg string, args ...any) {
	if exportFlags.Verbose {
		log.Printf(msg, args...)
	}
}
