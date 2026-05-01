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
	"archive/tar"
	"bufio"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"strings"

	"github.com/creachadair/command"
	"github.com/creachadair/ffs/file"
	"github.com/creachadair/ffs/filetree"
	"github.com/creachadair/ffs/fpath"
	"github.com/creachadair/ffstools/ffs/config"
	"github.com/creachadair/mds/value"
	"github.com/klauspost/compress/zstd"
)

var tarFlags struct {
	Compress bool   `flag:"compress,Compress output with zstd"`
	Root     string `flag:"root,Prefix all tar paths with this directory name"`
}

func runTarExport(env *command.Env, originPath string) (retErr error) {
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
	} else if f, err := os.OpenFile(exportFlags.Target, os.O_RDWR|os.O_EXCL|os.O_TRUNC|os.O_CREATE, 0700); err != nil {
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

	cfg := env.Config.(*config.Settings)
	return cfg.WithStore(env.Context(), func(s filetree.Store) error {
		of, err := s.OpenPath(env.Context(), originPath)
		if err != nil {
			return err
		}
		tdir := tarFlags.Root
		if tdir == "" && strings.Contains(originPath, "/") {
			tdir = path.Base(originPath)
		}

		tw := tar.NewWriter(w)
		mc = append(mc, tw.Close)
		return addFile(env, tw, of.File, tdir)
	})
}

type mcloser []func() error

func (m mcloser) Close() error {
	var errs []error
	for i := len(m) - 1; i >= 0; i-- {
		if err := m[i](); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// addFile is a demi-clone of [tar.Writer.AddFS], but with less OS-specific nonsense.
func addFile(env *command.Env, tw *tar.Writer, root *file.File, prefix string) error {
	return fpath.Walk(env.Context(), root, func(e fpath.Entry) error {
		if err := env.Context().Err(); err != nil {
			return err
		} else if e.Err != nil {
			return e.Err
		} else if e.File == root {
			return nil // skip
		}
		fi := e.File.FileInfo()
		dprintf(env, "a %s", path.Join(prefix, e.Path))

		// If this is a symlink, read the "file" contents out as the target.
		var linkTarget string
		if fi.Mode().Type() == fs.ModeSymlink {
			link, err := io.ReadAll(e.File.Cursor(env.Context()))
			if err != nil {
				return fmt.Errorf("read symlink: %w", err)
			}
			linkTarget = string(link)
			dprintf(env, "  link to %q", linkTarget)
		}

		// This does a bunch of nonsense we don't care about, but it handles the
		// ustar-specific encoding of file type bits that would be annoying to copy.
		// We'll adjust some of the results before writing the header, see below.
		h, err := tar.FileInfoHeader(lyingFileInfo{fi}, linkTarget)
		if err != nil {
			return err
		}

		// Replace the base name with the full path, including the prefix (if any).
		h.Name = path.Join(prefix, e.Path)
		if fi.Mode().IsDir() {
			h.Name += "/" // suffix directories with "/"
		}

		// Populate the owner and group IDs, as otherwise they will default to 0
		// and that makes the tar annoying to read when unpacked.
		fs := e.File.Stat()
		h.Uid = fs.OwnerID
		h.Uname = fs.OwnerName
		h.Gid = fs.GroupID
		h.Gname = fs.GroupName

		// If there are extended attributes, and we were asked to preserve them, do.
		if xa := e.File.XAttr(); xa.Len() != 0 && exportFlags.XAttr {
			dprintf(env, "  + %d extended attribute%s", xa.Len(), value.Cond(xa.Len() == 1, "", "s"))
			m := make(map[string]string)
			for _, name := range xa.Names() {
				m[name] = xa.Get(name)
			}
			//lint:ignore SA1019 This field is supposedly deprecated, but Go 1 protects us.
			h.Xattrs = m
		}
		if err := tw.WriteHeader(h); err != nil {
			return err
		}
		if fi.Mode().IsRegular() {
			_, err := io.Copy(tw, e.File.Cursor(env.Context()))
			return err
		}
		return nil
	})
}

// lyingFileInfo pretends to implement the [tar.FileInfoNames] interface so
// that the header constructor won't try to do name lookups on the system.
// But it just reports empty names, since we can fill those ourselves.
type lyingFileInfo struct{ fs.FileInfo }

func (lyingFileInfo) Uname() (string, error) { return "", nil }
func (lyingFileInfo) Gname() (string, error) { return "", nil }

func dprintf(w io.Writer, msg string, args ...any) {
	if exportFlags.Verbose {
		if !strings.HasSuffix(msg, "\n") {
			msg += "\n"
		}
		fmt.Fprintf(w, msg, args...)
	}
}
