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

package importlib

import (
	"archive/tar"
	"archive/zip"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/creachadair/ffs/blob"
	"github.com/creachadair/ffs/file"
	"github.com/creachadair/ffs/fpath"
	"github.com/klauspost/compress/zstd"
)

// ImportTarPath opens the specified path as a tar file and imports its
// contents as per [Config.ImportTar].
func (c Config) ImportTarPath(ctx context.Context, s blob.CAS, path string) (*file.File, error) {
	tr, tc, err := c.openTar(path)
	if err != nil {
		return nil, err
	}
	defer tc.Close()
	c.logPrintf("begin import tar: %q", path)
	return c.ImportTar(ctx, s, tr)
}

func (c Config) openTar(path string) (*tar.Reader, io.Closer, error) {
	var r io.Reader
	var fc io.Closer
	if path == "-" {
		r, fc = os.Stdin, os.Stdin
	} else if f, err := c.getFS().Open(path); err != nil {
		return nil, nil, err
	} else {
		r, fc = f, f
	}

	ext := strings.ToLower(filepath.Ext(path))
	if ext == ".zst" || ext == ".zstd" {
		dec, err := zstd.NewReader(r)
		if err != nil {
			panic(fmt.Sprintf("zstd reader: %v", err)) // should not be possible
		}
		r = dec
	}
	return tar.NewReader(r), fc, nil
}

// ImportZIPPath opens the specified path as a ZIP file and imports its
// contents as per [Config.ImportZIP].
func (c Config) ImportZIPPath(ctx context.Context, s blob.CAS, path string) (*file.File, error) {
	zr, zc, err := c.openZIP(path)
	if err != nil {
		return nil, err
	}
	defer zc.Close()
	c.logPrintf("begin import zip: %q", path)
	return c.ImportZIP(ctx, s, zr)
}

func (c Config) openZIP(path string) (*zip.Reader, io.Closer, error) {
	f, err := c.getFS().Open(path)
	if err != nil {
		return nil, nil, err
	}
	fi, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, nil, err
	}
	ra, ok := f.(io.ReaderAt)
	if !ok {
		f.Close()
		return nil, nil, errors.New("file is not random-access")
	}
	zr, err := zip.NewReader(ra, fi.Size())
	if err != nil {
		f.Close()
		return nil, nil, fmt.Errorf("input %q: %w", path, err)
	}
	return zr, f, nil
}

// ImportZIP imports the complete contents of zr into a new file tree in s, and
// returns the root of that tree. On success, the resulting root is flushed to
// storage, so its [File.Key] method will report the storage key.
func (c Config) ImportZIP(ctx context.Context, s blob.CAS, zr *zip.Reader) (*file.File, error) {
	// Since the contents of a ZIP may not all be under the same
	// directory, create a root directory to contain them all, so each
	// import has its own file tree.
	root := file.New(s, &file.NewOptions{
		Stat: &file.Stat{
			Mode:    fs.ModeDir | 0755,
			ModTime: time.Now(),
		},
		PersistStat: !c.OmitStat,
	})
	for _, entry := range zr.File {
		hf, err := zipHeaderToFile(ctx, entry, root)
		if err != nil {
			return nil, err
		}
		path := strings.TrimSuffix(entry.Name, "/") // directory names end in "/"
		if _, err := fpath.Set(ctx, root, path, &fpath.SetOptions{File: hf}); err != nil {
			return nil, fmt.Errorf("set %q: %w", path, err)
		}
		c.logPrintf("+ imported %s %q", hf.Stat().Mode, path)
	}
	if _, err := root.Flush(ctx); err != nil {
		return nil, err
	}
	return root, nil
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

// ImportTar imports the complete contents of tr into a new file tree in s, and
// returns the root of that tree. On success, the resulting root is flushed to
// storage, so its [File.Key] method will report the storage key.
func (c Config) ImportTar(ctx context.Context, s blob.CAS, tr *tar.Reader) (*file.File, error) {
	// Since the contents of a tar may not all be under the same
	// directory, create a root directory to contain them all, so each
	// import has its own file tree.
	root := file.New(s, &file.NewOptions{
		Stat: &file.Stat{
			Mode:    fs.ModeDir | 0755,
			ModTime: time.Now(),
			OwnerID: os.Getuid(),
			GroupID: os.Getgid(),
		},
		PersistStat: !c.OmitStat,
	})
	for {
		h, err := tr.Next()
		if errors.Is(err, io.EOF) {
			break // OK, the archive is ended
		} else if err != nil {
			return nil, err
		}
		hf, err := c.tarHeaderToFile(ctx, h, tr, root)
		if err != nil {
			return nil, err
		}
		path := strings.TrimSuffix(h.Name, "/") // directory names end in "/"
		if _, err := fpath.Set(ctx, root, path, &fpath.SetOptions{
			Create:  true,
			SetStat: setDirStat,
			File:    hf,
		}); err != nil {
			return nil, fmt.Errorf("set %q: %w", path, err)
		}
		c.logPrintf("+ imported %s %q", hf.Stat().Mode, path)
	}
	if _, err := root.Flush(ctx); err != nil {
		return nil, err
	}
	return root, nil
}

func (c Config) tarHeaderToFile(ctx context.Context, h *tar.Header, r io.Reader, root *file.File) (*file.File, error) {
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
	if c.IncludeXAttr {
		//lint:ignore SA1019 This field is supposedly deprecated, but Go 1 protects us.
		for name, value := range h.Xattrs {
			nf.XAttr().Set(name, value)
		}
	}
	if !fi.IsDir() {
		if err := nf.SetData(ctx, r); err != nil {
			return nil, fmt.Errorf("set file data: %w", err)
		}
	}
	return nf, nil
}

func setDirStat(s *file.Stat) {
	s.Mode = fs.ModeDir | 0755
	s.OwnerID = os.Getuid()
	s.GroupID = os.Getgid()
	s.ModTime = time.Now()
}
