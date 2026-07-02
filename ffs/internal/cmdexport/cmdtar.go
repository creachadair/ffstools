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
	"os"
	"path/filepath"
	"slices"

	"github.com/creachadair/command"
	"github.com/creachadair/ffs/filetree"
	"github.com/creachadair/ffstools/ffs/config"
	"github.com/klauspost/compress/zstd"
)

var tarFlags struct {
	Compress bool   `flag:"compress,Compress output with zstd"`
	Root     string `flag:"root,Prefix all output paths with this directory name"`
}

func runTarExport(env *command.Env, originPath string, rest ...string) (retErr error) {
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
	} else if f, err := os.OpenFile(exportFlags.Target, openFlags(), 0700); err != nil {
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
	tw := tar.NewWriter(w)
	mc = append(mc, tw.Close)

	cfg := env.Config.(*config.Settings)
	return cfg.WithStore(env.Context(), func(s filetree.Store) error {
		ec := exportConfig(env, tarFlags.Root)
		for _, originPath := range env.Args {
			of, err := s.OpenPath(env.Context(), originPath)
			if err != nil {
				return err
			}
			if err := ec.FileToTar(env.Context(), of, tw); err != nil {
				return fmt.Errorf("export %q: %w", originPath, err)
			}
		}
		return nil
	})
}

type mcloser []func() error

func (m mcloser) Close() error {
	var errs []error
	for _, close := range slices.Backward(m) {
		if err := close(); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}
