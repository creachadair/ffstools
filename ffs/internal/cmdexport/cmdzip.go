// Copyright 2025 Michael J. Fromberger. All Rights Reserved.
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
	"archive/zip"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path"
	"strings"

	"github.com/creachadair/command"
	"github.com/creachadair/ffs/file"
	"github.com/creachadair/ffs/filetree"
	"github.com/creachadair/ffs/fpath"
	"github.com/creachadair/ffstools/ffs/config"
)

var zipFlags struct {
	Root string `flag:"root,Prefix all output paths with this directory name"`
}

var dirStat = &file.Stat{Mode: fs.ModeDir | 0755}

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
		for _, originPath := range originPaths {
			of, err := s.OpenPath(env.Context(), originPath)
			if err != nil {
				return err
			}
			root := of.File
			if strings.Contains(originPath, "/") || zipFlags.Root != "" {
				p := path.Join(zipFlags.Root, path.Base(originPath))
				root = of.File.New(&file.NewOptions{Stat: dirStat})
				if _, err := fpath.Set(env.Context(), root, p, &fpath.SetOptions{
					Create:  true,
					SetStat: func(s *file.Stat) { s.Mode = fs.ModeDir | 0755 },
					File:    of.File,
				}); err != nil {
					return err
				}
			}
			if werr := addFileToZip(env, zw, root); werr != nil {
				zw.Close()
				return fmt.Errorf("copy to archive: %w", werr)
			}
		}
		if err := errors.Join(zw.Close(), f.Close()); err != nil {
			return fmt.Errorf("finalize archive: %w", err)
		}
		return nil
	})
}

func addFileToZip(env *command.Env, zw *zip.Writer, root *file.File) error {
	return fpath.Walk(env.Context(), root, func(e fpath.Entry) error {
		if err := env.Context().Err(); err != nil {
			return err
		} else if e.Err != nil {
			return e.Err
		} else if e.File == root {
			return nil // skip
		}
		fi := e.File.FileInfo()
		fh, err := zip.FileInfoHeader(fi)
		if err != nil {
			return fmt.Errorf("file info %q: %w", e.Path, err)
		}
		fh.Name = e.Path
		if fi.IsDir() {
			fh.Name += "/"
		}
		fh.Method = zip.Deflate
		h, err := zw.CreateHeader(fh)
		if err != nil {
			return fmt.Errorf("zip header %q: %w", e.Path, err)
		}
		if fi.IsDir() {
			dprintf(env, "dir: %s", e.Path)
			return nil
		}
		_, cerr := io.Copy(h, e.File.Cursor(env.Context()))
		if cerr == nil {
			dprintf(env, "+ %s: %s", fileType(fi), e.Path)
		}
		return cerr
	})
}

func fileType(fi fs.FileInfo) string {
	if fi.IsDir() {
		return "dir"
	} else if fi.Mode().IsRegular() {
		return "file"
	} else if fi.Mode()&fs.ModeSymlink != 0 {
		return "link"
	}
	return "other"
}
