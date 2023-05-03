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

package putlib

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io/fs"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/creachadair/ffs/blob"
	"github.com/creachadair/ffs/file"
	"github.com/creachadair/ffs/fpath"
	"github.com/creachadair/ffstools/ffs/config"
	"github.com/creachadair/taskgroup"
	"github.com/pkg/xattr"
)

var Default = Config{FilterName: ".ffsignore"}

type Config struct {
	Verbose    bool   // emit diagnostic output
	XAttr      bool   // capture extended attributes
	NoStat     bool   // do not capture stat metadata
	FilterName string // name of filter file to read
}

type state struct {
	s      blob.CAS
	path   string
	fi     fs.FileInfo
	filter *config.Filter
}

// PutFile puts a single file or symlink into the store.
func (c Config) PutFile(ctx context.Context, s blob.CAS, path string, fi fs.FileInfo) (*file.File, error) {
	return c.putFile(ctx, state{s: s, path: path, fi: fi})
}

func (c Config) putFile(ctx context.Context, st state) (*file.File, error) {
	f := file.New(st.s, &file.NewOptions{
		Name: st.fi.Name(),
		Stat: c.fileInfoToStat(st.fi),
	})

	// Extended attributes (if -xattr is set)
	if err := c.addExtAttrs(st.path, f); err != nil {
		return nil, err
	}

	if st.fi.Mode().IsRegular() {
		// Copy file contents.
		in, err := os.Open(st.path)
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
		tgt, err := os.Readlink(st.path)
		if err != nil {
			return nil, err
		} else if err := f.SetData(ctx, strings.NewReader(tgt)); err != nil {
			return nil, err
		}
	}
	return f, nil
}

// PutPath puts a single file, directory, or symlink into the store.  If path
// names a plain file or symlink, it calls PutFile.
func (c Config) PutPath(ctx context.Context, s blob.CAS, path string) (*file.File, error) {
	return c.putPath(ctx, state{s: s, path: path})
}

func (c Config) putPath(ctx context.Context, st state) (*file.File, error) {
	fi, err := os.Lstat(st.path)
	if err != nil {
		return nil, err
	}
	if !fi.IsDir() {
		// Non-directory files, symlinks, etc.
		st.fi = fi
		return c.putFile(ctx, st)
	}

	// Directory
	d := file.New(st.s, &file.NewOptions{
		Name: fi.Name(),
		Stat: c.fileInfoToStat(fi),
	})

	// Extended attributes (if -xattr is set)
	if err := c.addExtAttrs(st.path, d); err != nil {
		return nil, err
	}

	// Children
	elts, err := os.ReadDir(st.path)
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
			nf, err := filt.Load(sub)
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
		if filt.Match(sub) {
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

		kid, err := c.putPath(ctx, cp)
		if err != nil {
			return nil, err
		}
		d.Child().Set(e.name, kid)
	}

	// Process plain files in parallel.
	if len(files) != 0 {
		ctx, cancel := context.WithCancel(ctx)
		defer cancel()
		g, start := taskgroup.New(taskgroup.Trigger(cancel)).Limit(64)
		for _, e := range files {
			e := e
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
				kid, err := c.putFile(ctx, state{
					s: st.s, path: e.sub, fi: e.fi, filter: filt,
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
		ds := d.Stat()
		ds.ModTime = fi.ModTime()
		ds.Update()
	}
	return d, nil
}

func (c Config) addExtAttrs(path string, f *file.File) error {
	if !c.XAttr {
		return nil
	}
	names, err := xattr.LList(path)
	if err != nil {
		return fmt.Errorf("listing xattr: %w", err)
	}
	xa := f.XAttr()
	for _, name := range names {
		data, err := xattr.LGet(path, name)
		if err != nil {
			return fmt.Errorf("get xattr %q: %w", name, err)
		}
		xa.Set(name, string(data))
	}
	return nil
}

func (c Config) fileInfoToStat(fi fs.FileInfo) *file.Stat {
	if c.NoStat {
		return nil
	}
	owner, group := ownerAndGroup(fi)
	return &file.Stat{
		Mode:    fi.Mode(),
		ModTime: fi.ModTime(),
		OwnerID: owner,
		GroupID: group,
	}
}

// SetPath sets the specified root-key/path or file-key/path to the given
// target file. It returns the storage key of the resulting updated object.
//
// If path has only a root-key, the base file of that root is replaced.
// If path has only a file-keyi, it is an error.
func SetPath(ctx context.Context, s config.CAS, path string, tf *file.File) (string, error) {
	obase, orest := config.SplitPath(path)
	if orest == "." {
		orest = "" // setting root
	}

	of, err := config.OpenPath(ctx, s, obase) // N.B. No path; see below
	if err != nil {
		return "", err
	}

	// If orest == "", we are being asked to the target file of a root.
	if orest == "" {
		// If we don't have a root, we can't do anything.
		if of.Root == nil {
			return "", errors.New("cannot set the root of a file-key")
		}

		// Otherwise, flush the file and update the root.
		key, err := tf.Flush(ctx)
		if err != nil {
			return "", err
		}
		if key != of.Root.FileKey {
			of.Root.IndexKey = "" // invalidate the index, the key changed
		}
		of.Root.FileKey = key
		return key, of.Root.Save(ctx, of.RootKey, true) // replace
	}

	// Otherwise, we're hooking something below another object.
	if _, err := fpath.Set(ctx, of.Base, orest, &fpath.SetOptions{
		Create: true,
		SetStat: func(st *file.Stat) {
			if st.Mode == 0 {
				st.Mode = fs.ModeDir | 0755
			}
		},
		File: tf,
	}); err != nil {
		return "", err
	}

	return of.Flush(ctx)
}
