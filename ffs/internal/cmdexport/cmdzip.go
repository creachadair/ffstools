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
	"fmt"
	"io/fs"
	"os"

	"github.com/creachadair/command"
	"github.com/creachadair/ffs/file"
	"github.com/creachadair/ffs/filetree"
	"github.com/creachadair/ffs/fpath"
	"github.com/creachadair/ffstools/ffs/config"
)

var zipFlags struct {
	Root string `flag:"root,Prefix all paths with this directory name"`
}

var dirStat = &file.Stat{Mode: fs.ModeDir | 0755}

func runZipExport(env *command.Env, zipPath, originPath string) (retErr error) {
	f, err := os.OpenFile(zipPath, os.O_RDWR|os.O_EXCL|os.O_TRUNC|os.O_CREATE, 0600)
	if err != nil {
		return fmt.Errorf("output: %w", err)
	}
	defer f.Close()

	cfg := env.Config.(*config.Settings)
	return cfg.WithStore(env.Context(), func(s filetree.Store) error {
		of, err := s.OpenPath(env.Context(), originPath)
		if err != nil {
			return err
		}
		root := of.File
		if zipFlags.Root != "" {
			root = of.File.New(&file.NewOptions{Stat: dirStat})
			root.Child().Set(zipFlags.Root, of.File)
		}

		// TODO(creachadair): The AddFS method only supports plain files and
		// directories.  It is possible to encode symlinks in the binary format,
		// but I need to figure out how to express that, and it will probably
		// require re-implementing AddFS manually.

		zw := zip.NewWriter(f)
		if err := zw.AddFS(fpath.NewFS(env.Context(), root)); err != nil {
			return fmt.Errorf("copy to archive: %w", err)
		}
		if err := zw.Close(); err != nil { // N.B. does not close f
			return fmt.Errorf("finalize archive: %w", err)
		}
		return f.Close()
	})
}
