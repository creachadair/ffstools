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

package cmdexport

import (
	"context"
	"flag"
	"fmt"
	"io"
	"io/fs"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/creachadair/atomicfile"
	"github.com/creachadair/command"
	"github.com/creachadair/ffs/blob"
	"github.com/creachadair/ffs/file"
	"github.com/creachadair/ffs/file/root"
	"github.com/creachadair/ffs/fpath"
	"github.com/creachadair/ffstools/ffs/config"
	"github.com/creachadair/taskgroup"
	"github.com/pkg/xattr"
)

var exportFlags struct {
	Stat    bool
	XAttr   bool
	Verbose bool
	Target  string
	Update  bool
}

var Command = &command.C{
	Name: "export",
	Usage: `root:<root-key>[/path]
<file-key>[/path]`,
	Help: `Export a file tree to the local filesystem.`,

	SetFlags: func(_ *command.Env, fs *flag.FlagSet) {
		fs.BoolVar(&exportFlags.Stat, "stat", false, "Update permissions and modification times")
		fs.BoolVar(&exportFlags.XAttr, "xattr", false, "Restore extended attributes")
		fs.BoolVar(&exportFlags.Verbose, "v", false, "Enable verbose logging")
		fs.BoolVar(&exportFlags.Update, "update", false, "Update target if it exists")
		fs.StringVar(&exportFlags.Target, "to", "", "Export to this path (required)")
	},
	Run: runExport,
}

func runExport(env *command.Env, args []string) error {
	if len(args) == 0 || args[0] == "" {
		return env.Usagef("missing required object path")
	} else if len(args) > 1 {
		return env.Usagef("extra arguments: %q", args[1:])
	} else if exportFlags.Target == "" {
		return env.Usagef("missing required -to path")
	}

	// Create leading components of the target directory path, as required.
	if err := os.MkdirAll(filepath.Dir(exportFlags.Target), 0700); err != nil {
		return err
	}

	cfg := env.Config.(*config.Settings)
	return cfg.WithStore(cfg.Context, func(s blob.CAS) error {
		parts := strings.SplitN(args[0], "/", 2)
		f, err := openFile(cfg.Context, s, parts[0])
		if err != nil {
			return err
		}
		if len(parts) == 2 {
			f, err = fpath.Open(cfg.Context, f, parts[1])
		}
		if err != nil {
			return err
		}
		cctx, cancel := context.WithCancel(cfg.Context)
		defer cancel()
		g, start := taskgroup.New(taskgroup.Trigger(cancel)).Limit(32)

		g.Go(func() error {
			return fpath.Walk(cctx, f, func(e fpath.Entry) error {
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
		logPrintf("Create directory %q", path)
		if err := os.Mkdir(path, 0700); err != nil {
			if !exportFlags.Update || !os.IsExist(err) {
				return err
			}
		}
	} else if mode.Type()&fs.ModeSymlink != 0 {
		logPrintf("Create symlink %q", path)
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
		logPrintf("Export %q", path)
		if err := copyFile(ctx, f, path); err != nil {
			return err
		}
	}

	// Restore permissions and modification times, if requested and available.
	if exportFlags.Stat && f.Stat().Persistent() && !link {
		stat := f.Stat()
		logPrintf("Restore %q mode %v and modtime %v",
			path, stat.Mode.Perm(), stat.ModTime.Format(time.RFC3339))

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
		var xerr error
		f.XAttr().List(func(key, value string) {
			if xerr == nil {
				logPrintf("Restore %q xattr %q", path, key)
				xerr = xattr.LSet(path, key, []byte(value))
			}
		})
		if xerr != nil {
			return fmt.Errorf("setting xattrs: %w", xerr)
		}
	}
	return nil
}

func copyFile(ctx context.Context, f *file.File, path string) error {
	_, err := atomicfile.WriteAll(path, f.Cursor(ctx), 0600)
	return err
}

func linkFile(ctx context.Context, f *file.File, path string) error {
	target, err := io.ReadAll(f.Cursor(ctx))
	if err != nil {
		return fmt.Errorf("reading link target: %w", err)
	}
	return os.Symlink(string(target), path)
}

func openFile(ctx context.Context, s blob.CAS, spec string) (*file.File, error) {
	if strings.HasPrefix(spec, "@") {
		rp, err := root.Open(ctx, config.Roots(s), spec[1:])
		if err != nil {
			return nil, err
		}
		return rp.File(ctx, s)
	}
	key, err := config.ParseKey(spec)
	if err != nil {
		return nil, err
	}
	return file.Open(ctx, s, key)
}

func logPrintf(msg string, args ...interface{}) {
	if exportFlags.Verbose {
		log.Printf(msg, args...)
	}
}
