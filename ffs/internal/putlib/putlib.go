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
	"fmt"
	"io/fs"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/creachadair/ffs/blob"
	"github.com/creachadair/ffs/file"
	"github.com/creachadair/taskgroup"
	"github.com/pkg/xattr"
)

var Default = Config{}

type Config struct {
	Verbose bool // emit diagnostic output
	XAttr   bool // capture extended attributes
	NoStat  bool // do not capture stat metadata
}

// PutFile puts a single file or symlink into the store.
func (c Config) PutFile(ctx context.Context, s blob.CAS, path string, fi fs.FileInfo) (*file.File, error) {
	f := file.New(s, &file.NewOptions{
		Name: fi.Name(),
		Stat: c.fileInfoToStat(fi),
	})

	// Extended attributes (if -xattr is set)
	if err := c.addExtAttrs(path, f); err != nil {
		return nil, err
	}

	if fi.Mode().IsRegular() {
		// Copy file contents.
		in, err := os.Open(path)
		if err != nil {
			return nil, err
		}
		defer in.Close()
		r := bufio.NewReaderSize(in, 1<<20)
		if err := f.SetData(ctx, r); err != nil {
			return nil, fmt.Errorf("copying data: %w", err)
		}
	} else if fi.Mode()&fs.ModeSymlink != 0 {
		// Write symbolic link target as file content.
		tgt, err := os.Readlink(path)
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
	fi, err := os.Lstat(path)
	if err != nil {
		return nil, err
	}
	if !fi.IsDir() {
		// Non-directory files, symlinks, etc.
		return c.PutFile(ctx, s, path, fi)
	}
	if c.Verbose {
		log.Printf("enter %q", path)
	}

	// Directory
	d := file.New(s, &file.NewOptions{
		Name: fi.Name(),
		Stat: c.fileInfoToStat(fi),
	})

	// Extended attributes (if -xattr is set)
	if err := c.addExtAttrs(path, d); err != nil {
		return nil, err
	}

	// Children
	elts, err := os.ReadDir(path)
	if err != nil {
		return nil, err
	}

	type entry struct {
		sub  string
		name string
		fi   fs.FileInfo
		kid  *file.File
	}

	// Partition the contents of the directory into plain files and directories.
	var files, dirs []*entry
	for _, elt := range elts {
		sub := filepath.Join(path, elt.Name())
		if elt.IsDir() {
			dirs = append(dirs, &entry{sub: sub, name: elt.Name()})
		} else if t := elt.Type(); t != 0 && (t&fs.ModeSymlink == 0) {
			continue // e.g., socket, pipe, device, fifo, etc.
		} else if fi, err := elt.Info(); err != nil {
			return nil, err
		} else {
			files = append(files, &entry{sub: sub, name: elt.Name(), fi: fi})
		}
	}

	// Process subdirectories serially. We do this so that the recurrence does
	// not explode concurrency.
	for _, e := range dirs {
		kid, err := c.PutPath(ctx, s, e.sub)
		if err != nil {
			return nil, err
		}
		d.Child().Set(e.name, kid)
	}

	// Process plain files in parallel.
	if len(files) != 0 {
		if c.Verbose {
			log.Printf("in %q: storing %d files", path, len(files))
		}
		ctx, cancel := context.WithCancel(ctx)
		defer cancel()
		g, start := taskgroup.New(taskgroup.Trigger(cancel)).Limit(64)
		for _, e := range files {
			e := e
			start(func() error {
				if c.Verbose {
					log.Printf("copying %d bytes from %q", e.fi.Size(), e.name)
					if e.fi.Size() > 1<<20 {
						st := time.Now()
						defer func() {
							log.Printf("finished %q [%v elapsed]",
								e.name, time.Since(st).Truncate(time.Millisecond))
						}()
					}
				}
				kid, err := c.PutFile(ctx, s, e.sub, e.fi)
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
