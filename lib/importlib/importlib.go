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

// Package importlib implements common plumbing for copying file trees from a
// local filesystem into FFS representation.
package importlib

import (
	"bufio"
	"context"
	"fmt"
	"io/fs"
	"log"
	"path/filepath"
	"strings"
	"time"

	"github.com/creachadair/ffs/blob"
	"github.com/creachadair/ffs/file"
	"github.com/creachadair/taskgroup"
)

var Default = Config{FilterName: ".ffsignore"}

// Config carries settings for storing files into an FFS store.
type Config struct {
	Verbose      bool   // emit diagnostic output
	IncludeXAttr bool   // capture extended attributes
	OmitStat     bool   // do not capture stat metadata
	FilterName   string // name of filter file to read

	// A filesystem implementation to read from, or nil.
	// If it is nil, the config uses the standard library's [os] package.
	// It may optionally also implement [fs.ReadLinkFS] if symlinks are supported.
	// It may optionally also implement [XAttrFS] if extended attributes are supported.
	FS fs.ReadDirFS
}

func (c Config) getFS() fs.ReadDirFS {
	if c.FS == nil {
		return osFS{}
	}
	return c.FS
}

func (c Config) logPrintf(msg string, args ...any) {
	if c.Verbose {
		log.Printf(msg, args...)
	}
}

type state struct {
	s      blob.CAS
	path   string
	fi     fs.FileInfo
	filter *Filter
	fs     fs.FS
}

func (c Config) importFile(ctx context.Context, st state) (*file.File, error) {
	f := file.New(st.s, c.fileInfoToOptions(st.fi))

	// Extended attributes (if --xattr is set)
	if err := c.addExtAttrs(st, f); err != nil {
		return nil, err
	}

	if st.fi.Mode().IsRegular() {
		// Copy file contents.
		in, err := st.fs.Open(st.path)
		if err != nil {
			return nil, err
		}
		defer in.Close()
		r := bufio.NewReaderSize(in, 1<<20)
		if err := f.SetData(ctx, r); err != nil {
			return nil, fmt.Errorf("copying data: %w", err)
		}
	} else if st.fi.Mode()&fs.ModeSymlink != 0 {
		// Write symbolic link target as file content.
		tgt, err := fs.ReadLink(st.fs, st.path)
		if err != nil {
			return nil, err
		} else if err := f.SetData(ctx, strings.NewReader(tgt)); err != nil {
			return nil, err
		}
	}
	return f, nil
}

// ImportPath imports a single file, directory, or symlink into the store.
// If path names a directory, its contents are imported recursively.
func (c Config) ImportPath(ctx context.Context, s blob.CAS, path string) (*file.File, error) {
	return c.importPath(ctx, state{s: s, path: path, fs: c.getFS()})
}

func (c Config) importPath(ctx context.Context, st state) (*file.File, error) {
	fi, err := fs.Lstat(st.fs, st.path)
	if err != nil {
		return nil, err
	}
	if !fi.IsDir() {
		// Non-directory files, symlinks, etc.
		st.fi = fi
		return c.importFile(ctx, st)
	}

	// Directory
	d := file.New(st.s, c.fileInfoToOptions(fi))

	// Extended attributes (if -xattr is set)
	if err := c.addExtAttrs(st, d); err != nil {
		return nil, err
	}

	// Children
	elts, err := fs.ReadDir(st.fs, st.path)
	if err != nil {
		return nil, err
	}

	type entry struct {
		sub  string
		name string
		fi   fs.FileInfo
		kid  *file.File
	}

	// Precheck for filter rules.
	filt := st.filter
	for _, elt := range elts {
		if elt.Name() == c.FilterName {
			sub := filepath.Join(st.path, elt.Name())
			nf, err := filt.LoadFile(sub)
			if err != nil {
				return nil, fmt.Errorf("loading filter: %w", err)
			} else if c.Verbose {
				log.Printf("load filter rules: %s", sub)
			}
			filt = nf
			break
		}
	}

	// Partition the contents of the directory into plain files and directories.
	var files, dirs []*entry
	for _, elt := range elts {
		sub := filepath.Join(st.path, elt.Name())
		if filt.Exclude(sub) {
			if c.Verbose {
				log.Printf("skip (filtered): %s", sub)
			}
			continue
		} else if elt.IsDir() {
			dirs = append(dirs, &entry{sub: sub, name: elt.Name()})
		} else if t := elt.Type(); t != 0 && (t&fs.ModeSymlink == 0) {
			continue // e.g., socket, pipe, device, fifo, etc.
		} else if fi, err := elt.Info(); err != nil {
			return nil, err
		} else {
			files = append(files, &entry{sub: sub, name: elt.Name(), fi: fi})
		}
	}
	if c.Verbose {
		log.Printf("dir: %s (%d files, %d dirs)", st.path, len(files), len(dirs))
	}

	// Process subdirectories serially. We do this so that the recurrence does
	// not explode concurrency.
	for _, e := range dirs {
		cp := st
		cp.path, cp.filter = e.sub, filt

		kid, err := c.importPath(ctx, cp)
		if err != nil {
			return nil, err
		}
		d.Child().Set(e.name, kid)
	}

	// Process plain files in parallel.
	if len(files) != 0 {
		ctx, cancel := context.WithCancel(ctx)
		defer cancel()
		g, start := taskgroup.New(cancel).Limit(64)
		for _, e := range files {
			start(func() error {
				if c.Verbose {
					log.Printf("file: %s (%d bytes)", e.name, e.fi.Size())
					if e.fi.Size() > 1<<20 {
						begin := time.Now()
						defer func() {
							log.Printf("file done: %s [%v elapsed]",
								e.name, time.Since(begin).Truncate(time.Millisecond))
						}()
					}
				}
				kid, err := c.importFile(ctx, state{
					s: st.s, path: e.sub, fi: e.fi, filter: filt, fs: c.getFS(),
				})
				if err != nil {
					return err
				}
				e.kid = kid
				return nil
			})
		}
		if err := g.Wait(); err != nil {
			return nil, err
		}
		for _, e := range files {
			d.Child().Set(e.name, e.kid)
		}
	}
	if len(files) != 0 || len(dirs) != 0 {
		d.Stat().WithModTime(fi.ModTime()).Update()
	}
	return d, nil
}

func (c Config) addExtAttrs(st state, f *file.File) error {
	if !c.IncludeXAttr {
		return nil
	}
	xfs, ok := st.fs.(XAttrFS)
	if !ok {
		return nil // no extended attribute support
	}
	names, err := xfs.ListXAttr(st.path)
	if err != nil {
		return fmt.Errorf("listing xattr: %w", err)
	}
	xa := f.XAttr()
	for _, name := range names {
		data, err := xfs.GetXAttr(st.path, name)
		if err != nil {
			return fmt.Errorf("get xattr %q: %w", name, err)
		}
		xa.Set(name, string(data))
	}
	return nil
}

func (c Config) fileInfoToOptions(fi fs.FileInfo) *file.NewOptions {
	if c.OmitStat {
		return &file.NewOptions{Name: fi.Name()} // PersistStat == false
	}
	owner, group := ownerAndGroup(fi)
	return &file.NewOptions{
		Name: fi.Name(),
		Stat: &file.Stat{
			Mode:    fi.Mode(),
			ModTime: fi.ModTime(),
			OwnerID: owner,
			GroupID: group,
		},
		PersistStat: true,
	}
}
