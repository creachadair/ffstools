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
	"os"

	"github.com/creachadair/command"
	"github.com/creachadair/ffs/filetree"
	"github.com/creachadair/ffstools/ffs/config"
)

var zipFlags struct {
	Root string `flag:"root,Prefix all output paths with this directory name"`
}

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
		ec := exportConfig(env, zipFlags.Root)
		for _, originPath := range originPaths {
			of, err := s.OpenPath(env.Context(), originPath)
			if err != nil {
				return err
			}
			if werr := ec.FileToZIP(env.Context(), of, zw); werr != nil {
				zw.Close()
				return fmt.Errorf("copy to archive: %w", err)
			}
		}
		if err := errors.Join(zw.Close(), f.Close()); err != nil {
			return fmt.Errorf("finalize archive: %w", err)
		}
		return nil
	})
}
