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
	"archive/tar"
	"archive/zip"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/creachadair/command"
	"github.com/creachadair/ffs/file"
	"github.com/creachadair/ffs/filetree"
	"github.com/creachadair/ffs/fpath"
	"github.com/creachadair/ffstools/ffs/config"
	"github.com/creachadair/ffstools/lib/putlib"
	"github.com/creachadair/flax"
	"github.com/klauspost/compress/zstd"
)

const intoHelp = `

With --into, the resulting file tree is stored under the specified path
of the form <root-key>/<path> or @<file-key>/<path>. In this form, only
one input path is allowed.`

var importFlags struct {
	Target  string `flag:"into,Store the resulting object under this root/path or file/path"`
	Verbose bool   `flag:"v,Enable verbose logging"`
}

var Command = &command.C{
	Name:     "import",
	Help:     `Import one or more file trees from archives.`,
	SetFlags: command.Flags(flax.MustBind, &importFlags),
	Commands: []*command.C{{
		Name:  "tar",
		Usage: "<path> ...",
		Help: `Import file trees from Unix tape archive (tar) files.

Paths ending in ".zst" or ".zstd" are automatically decompressed.
Use "-" for the path to read an (uncompressed) archive from stdin.` + intoHelp,
		Run: command.Adapt(runImportTar),
	}, {
		Name:  "zip",
		Usage: "<path> ...",
		Help:  `Import file trees from ZIP archive files.` + intoHelp,
		Run:   command.Adapt(runImportZIP),
	}},
}

func runImportTar(env *command.Env, srcPath string, rest []string) error {
	if importFlags.Target != "" && len(rest) != 0 {
		return env.Usagef("only one path is allowed when --into is set")
	}

	cfg := env.Config.(*config.Settings)
	return cfg.WithStore(env.Context(), func(s filetree.Store) error {
		var lastRoot *file.File
		for _, path := range env.Args {
			tf, c, err := openTar(path)
			if err != nil {
				return err
			}
			logPrintf("begin import tar %q", path)

			// Since the contents of a tar may not all be under the same
			// directory, create a root directory to contain them all, so each
			// import has its own file tree.
			root := file.New(s.Files(), &file.NewOptions{
				Stat: &file.Stat{
					Mode:    fs.ModeDir | 0755,
					ModTime: time.Now(),
					OwnerID: os.Getuid(),
					GroupID: os.Getgid(),
				},
				PersistStat: true,
			})
			for {
				h, err := tf.Next()
				if errors.Is(err, io.EOF) {
					c.Close()
					break
				} else if err != nil {
					c.Close()
					return fmt.Errorf("input %q: %w", path, err)
				}
				hf, err := tarHeaderToFile(env.Context(), h, tf, root)
				if err != nil {
					c.Close()
					return err
				}
				path := strings.TrimSuffix(h.Name, "/") // directory names end in "/"
				if _, err := fpath.Set(env.Context(), root, path, &fpath.SetOptions{
					Create:  true,
					SetStat: setDirStat,
					File:    hf,
				}); err != nil {
					c.Close()
					return fmt.Errorf("set %q: %w", path, err)
				}
				logPrintf("+ imported %s %q", hf.Stat().Mode, path)
			}
			c.Close()
			key, err := root.Flush(env.Context())
			if err != nil {
				return err
			}
			fmt.Printf("import: %s\n", config.FormatKey(key))
			lastRoot = root
		}

		// If the --into flag was set, then we know there was exactly one import
		// (because we checked that at the top) and lastRoot is its root (or else
		// we would not have gotten here).
		if importFlags.Target != "" {
			key, err := putlib.SetPath(env.Context(), s, importFlags.Target, lastRoot)
			if err != nil {
				return err
			}
			fmt.Printf("set: %s\n", config.FormatKey(key))
		}
		return nil
	})
}

func runImportZIP(env *command.Env, srcPath string, rest []string) error {
	if importFlags.Target != "" && len(rest) != 0 {
		return env.Usagef("only one path is allowed when --into is set")
	}

	cfg := env.Config.(*config.Settings)
	return cfg.WithStore(env.Context(), func(s filetree.Store) error {
		var lastRoot *file.File
		for _, path := range env.Args {
			zf, c, err := openZIP(path)
			if err != nil {
				return err
			}
			log.Printf("begin import zip: %q", path)

			// Since the contents of a ZIP may not all be under the same
			// directory, create a root directory to contain them all, so each
			// import has its own file tree.
			root := file.New(s.Files(), &file.NewOptions{
				Stat: &file.Stat{
					Mode:    fs.ModeDir | 0755,
					ModTime: time.Now(),
				},
				PersistStat: true,
			})
			for _, entry := range zf.File {
				hf, err := zipHeaderToFile(env.Context(), entry, root)
				if err != nil {
					c.Close()
					return err
				}
				path := strings.TrimSuffix(entry.Name, "/") // directory names end in "/"
				if _, err := fpath.Set(env.Context(), root, path, &fpath.SetOptions{File: hf}); err != nil {
					c.Close()
					return fmt.Errorf("set %q: %w", path, err)
				}
				logPrintf("+ imported %s %q", hf.Stat().Mode, path)
			}
			c.Close()
			key, err := root.Flush(env.Context())
			if err != nil {
				return err
			}
			fmt.Printf("import: %s\n", config.FormatKey(key))
			lastRoot = root
		}

		// If the --into flag was set, then we know there was exactly one import
		// (because we checked that at the top) and lastRoot is its root (or else
		// we would not have gotten here).
		if importFlags.Target != "" {
			key, err := putlib.SetPath(env.Context(), s, importFlags.Target, lastRoot)
			if err != nil {
				return err
			}
			fmt.Printf("set: %s\n", config.FormatKey(key))
		}
		return nil
	})
}

func tarHeaderToFile(ctx context.Context, h *tar.Header, r io.Reader, root *file.File) (*file.File, error) {
	fi := h.FileInfo()
	nf := root.New(&file.NewOptions{
		Name: fi.Name(),
		Stat: &file.Stat{
			Mode:      fi.Mode(),
			ModTime:   fi.ModTime(),
			OwnerID:   h.Uid,
			OwnerName: h.Uname,
			GroupID:   h.Gid,
			GroupName: h.Gname,
		},
	})
	if !fi.IsDir() {
		if err := nf.SetData(ctx, r); err != nil {
			return nil, fmt.Errorf("set file data: %w", err)
		}
	}
	return nf, nil
}

func zipHeaderToFile(ctx context.Context, f *zip.File, root *file.File) (*file.File, error) {
	fi := f.FileInfo()
	nf := root.New(&file.NewOptions{
		Name: fi.Name(),
		Stat: &file.Stat{
			Mode:    fi.Mode(),
			ModTime: fi.ModTime(),

			// ZIP files do not record owner/group IDs or names, so use the ambient.
			OwnerID: os.Getuid(),
			GroupID: os.Getgid(),
		},
	})
	if !fi.IsDir() {
		rc, err := f.Open()
		if err != nil {
			return nil, fmt.Errorf("read contents: %w", err)
		}
		defer rc.Close()
		return nf, nf.SetData(ctx, rc)
	}
	return nf, nil
}

func openTar(path string) (*tar.Reader, io.Closer, error) {
	var r io.Reader
	var c io.Closer
	if path == "-" {
		r, c = os.Stdin, os.Stdin
	} else if f, err := os.Open(path); err != nil {
		return nil, nil, err
	} else {
		r, c = f, f
	}

	ext := strings.ToLower(filepath.Ext(path))
	if ext == ".zst" || ext == ".zstd" {
		dec, err := zstd.NewReader(r)
		if err != nil {
			panic(fmt.Sprintf("zstd reader: %v", err)) // should not be possible
		}
		r = dec
	}
	return tar.NewReader(r), c, nil
}

func openZIP(path string) (*zip.Reader, io.Closer, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, nil, err
	}
	fi, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, nil, err
	}
	zr, err := zip.NewReader(f, fi.Size())
	if err != nil {
		f.Close()
		return nil, nil, fmt.Errorf("input %q: %w", path, err)
	}
	return zr, f, nil
}

func setDirStat(s *file.Stat) {
	s.Mode = fs.ModeDir | 0755
	s.OwnerID = os.Getuid()
	s.GroupID = os.Getgid()
	s.ModTime = time.Now()
}

func logPrintf(msg string, args ...any) {
	if importFlags.Verbose {
		log.Printf(msg, args...)
	}
}
