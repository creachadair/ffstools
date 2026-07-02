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
	"archive/zip"
	"context"
	"fmt"
	"io/fs"
	"os"
	"strings"
	"time"

	"github.com/creachadair/ffs/blob"
	"github.com/creachadair/ffs/file"
	"github.com/creachadair/ffs/fpath"
)

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
