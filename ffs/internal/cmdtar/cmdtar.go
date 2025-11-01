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

package cmdtar

import (
	"archive/tar"
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path"
	"path/filepath"

	"github.com/creachadair/command"
	"github.com/creachadair/ffs/file"
	"github.com/creachadair/ffs/filetree"
	"github.com/creachadair/ffs/fpath"
	"github.com/creachadair/ffstools/ffs/config"
	"github.com/creachadair/flax"
	"github.com/klauspost/compress/zstd"
)

var tarFlags struct {
	Target   string `flag:"to,Export to this path"`
	Compress bool   `flag:"compress,Compress output with zstd"`
	Root     string `flag:"root,Prefix all tar paths with this directory name"`
	XAttr    bool   `flag:"xattr,Include extended attributes"`
}

var Command = &command.C{
	Name: "tar",
	Usage: `<root-key>[/path/...]
@<file-key>[/path/...]`,
	Help: `
Export a file tree to a tar archive.

Recursively export the file indicated by the selected root or file storage key to
a tar stream. If --to is set, the output is written to that file, which is created
if necessary; otherwise the output is written to stdout.

If --compress is true, or if the --to filename ends in ".zst" or ".zstd", the output
is compressed with zstd.`,

	SetFlags: command.Flags(flax.MustBind, &tarFlags),
	Run:      command.Adapt(runTarExport),
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
	if tarFlags.Target == "" {
		w = os.Stdout
	} else if f, err := os.OpenFile(tarFlags.Target, os.O_RDWR|os.O_EXCL|os.O_TRUNC|os.O_CREATE, 0700); err != nil {
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
	ext := filepath.Ext(tarFlags.Target)
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
		of, err := filetree.OpenPath(env.Context(), s, originPath)
		if err != nil {
			return err
		}

		tw := tar.NewWriter(w)
		mc = append(mc, tw.Close)
		return addFile(env.Context(), tw, of.File, tarFlags.Root)
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
func addFile(ctx context.Context, tw *tar.Writer, root *file.File, prefix string) error {
	return fpath.Walk(ctx, root, func(e fpath.Entry) error {
		if ctx.Err() != nil {
			return ctx.Err()
		} else if e.Err != nil {
			return e.Err
		} else if e.File == root {
			return nil // skip
		}
		fi := e.File.FileInfo()

		// If this is a symlink, read the "file" contents out as the target.
		var linkTarget string
		if fi.Mode().Type() == fs.ModeSymlink {
			link, err := io.ReadAll(e.File.Cursor(ctx))
			if err != nil {
				return fmt.Errorf("read symlink: %w", err)
			}
			linkTarget = string(link)
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
		if xa := e.File.XAttr(); xa.Len() != 0 && tarFlags.XAttr {
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
			_, err := io.Copy(tw, e.File.Cursor(ctx))
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
