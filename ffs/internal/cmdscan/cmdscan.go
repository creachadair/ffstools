// Copyright 2024 Michael J. Fromberger. All Rights Reserved.
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

// Package cmdscan implements the "ffs scan" subcommand.
package cmdscan

import (
	"fmt"
	"time"

	"github.com/creachadair/command"
	"github.com/creachadair/ffstools/ffs/config"
	"github.com/creachadair/ffstools/lib/scanlib"
	"github.com/creachadair/flax"
)

var scanFlags struct {
	Keys bool `flag:"keys,Print keys to stdout"`
}

var Command = &command.C{
	Name: "scan",
	Usage: `@<file-key>[/path/...] ...
<root-key>[/path/...] ...`,
	Help:     `Scan blobs reachable from the specified file trees.`,
	SetFlags: command.Flags(flax.MustBind, &scanFlags),
	Run:      command.Adapt(runScan),
}

func runScan(env *command.Env, sourceKeys ...string) error {
	cfg := env.Config.(*config.Settings)
	return cfg.WithStore(env.Context(), func(src config.CAS) error {
		// Find all the objects reachable from the specified starting points.
		worklist := scanlib.NewScanner(src)
		for _, elt := range sourceKeys {
			of, err := config.OpenPath(env.Context(), src, elt)
			if err != nil {
				return err
			}

			scanStart := time.Now()
			if of.Root != nil && of.Base == of.File {
				fmt.Fprintf(env, "Scanning data reachable from root %q", of.RootKey)
				err = worklist.Root(env.Context(), of.RootKey, of.Root)
			} else {
				fmt.Fprintf(env, "Scanning data reachable from file %s", config.FormatKey(of.FileKey))
				err = worklist.File(env.Context(), of.File)
			}
			fmt.Fprintf(env, " [%v elapsed]\n", time.Since(scanStart).Round(time.Millisecond))
			if err != nil {
				return err
			}
		}
		stats := worklist.Stats()
		fmt.Fprintf(env, "Found %d reachable objects (%d roots, %d files, %d blobs)\n", worklist.Len(),
			stats.NumRoots, stats.NumFiles, stats.NumBlobs)

		if scanFlags.Keys {
			for chunk := range worklist.Chunks(256) {
				for _, key := range chunk {
					fmt.Println(config.FormatKey(key))
				}
			}
		}
		return nil
	})
}
