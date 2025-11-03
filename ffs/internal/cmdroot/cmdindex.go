// Copyright 2021 Michael J. Fromberger. All Rights Reserved.
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

package cmdroot

import (
	"fmt"
	"time"

	"github.com/creachadair/command"
	"github.com/creachadair/ffs/file"
	"github.com/creachadair/ffs/file/root"
	"github.com/creachadair/ffs/file/wiretype"
	"github.com/creachadair/ffs/filetree"
	"github.com/creachadair/ffs/index"
	"github.com/creachadair/ffstools/ffs/config"
	"github.com/creachadair/mds/mapset"
)

var indexFlags struct {
	Force   bool `flag:"f,Force reindexing"`
	Discard bool `flag:"discard,Discard cached index if present"`
}

func runIndex(env *command.Env) error {
	if len(env.Args) == 0 {
		return env.Usagef("missing required <root-key>")
	} else if indexFlags.Force && indexFlags.Discard {
		return env.Usagef("the --discard and --force flags are mutually exclusive")
	}

	cfg := env.Config.(*config.Settings)
	return cfg.WithStore(env.Context(), func(s filetree.Store) error {
		for _, key := range env.Args {
			rp, err := root.Open(env.Context(), s.Roots(), key)
			if err != nil {
				return err
			}
			if rp.IndexKey != "" {
				if indexFlags.Discard {
					rp.IndexKey = ""
					if err := rp.Save(env.Context(), key); err != nil {
						return fmt.Errorf("saving root: %w", err)
					}
					fmt.Fprintf(env, "Removed cached index for %q\n", key)
					continue
				} else if !indexFlags.Force {
					fmt.Fprintf(env, "Root %q is already indexed\n", key)
					continue
				}

				// Reaching here, we are asked to compute a new index.
			} else if indexFlags.Discard {
				fmt.Fprintf(env, "Root %q has no cached index (OK)\n", key)
				continue
			}

			fp, err := rp.File(env.Context(), s.Files())
			if err != nil {
				return err
			}

			fmt.Fprintf(env, "Scanning data reachable from %q (%s)...\n", key, config.FormatKey(rp.FileKey))
			scanned := mapset.New[string]()
			start := time.Now()
			if err := fp.Scan(env.Context(), func(si file.ScanItem) bool {
				key := si.Key()
				if scanned.Has(key) {
					return false // don't re-scan repeats of the same file
				}
				scanned.Add(key)
				scanned.Add(si.Data().Keys()...)
				return true
			}); err != nil {
				return fmt.Errorf("scanning %q: %w", key, err)
			}
			fmt.Fprintf(env, "Finished scanning %d objects [%v elapsed]\n",
				len(scanned), time.Since(start).Truncate(10*time.Millisecond))

			// Now that we know the size of the set, pack the keys into the index.
			idx := index.New(len(scanned), &index.Options{FalsePositiveRate: 0.01})
			for key := range scanned {
				idx.Add(key)
			}

			rp.IndexKey, err = wiretype.Save(env.Context(), s.Files(), &wiretype.Object{
				Value: &wiretype.Object_Index{Index: index.Encode(idx)},
			})
			if err != nil {
				return fmt.Errorf("saving index: %w", err)
			}
			if err := rp.Save(env.Context(), key); err != nil {
				return err
			}
		}
		return nil
	})
}
