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
	Force bool `flag:"f,Force reindexing"`
}

func runIndex(env *command.Env) error {
	if len(env.Args) == 0 {
		return env.Usagef("missing required <root-key>")
	}

	cfg := env.Config.(*config.Settings)
	return cfg.WithStore(env.Context(), func(s filetree.Store) error {
		n, err := s.Files().Len(env.Context())
		if err != nil {
			return err
		}
		for _, key := range env.Args {
			rp, err := root.Open(env.Context(), s.Roots(), key)
			if err != nil {
				return err
			}
			if rp.IndexKey != "" && !indexFlags.Force {
				fmt.Fprintf(env, "Root %q is already indexed\n", key)
				continue
			}
			fp, err := rp.File(env.Context(), s.Files())
			if err != nil {
				return err
			}

			fmt.Fprintf(env, "Scanning data reachable from %q (%s)...\n", key, config.FormatKey(rp.FileKey))
			scanned := mapset.New[string]()
			idx := index.New(int(n), &index.Options{FalsePositiveRate: 0.01})
			start := time.Now()
			if err := fp.Scan(env.Context(), func(si file.ScanItem) bool {
				key := si.Key()
				if scanned.Has(key) {
					return false // don't re-index repeats of the same file
				}
				scanned.Add(key)
				idx.Add(key)
				for _, dkey := range si.Data().Keys() {
					idx.Add(dkey)
				}
				return true
			}); err != nil {
				return fmt.Errorf("scanning %q: %w", key, err)
			}
			fmt.Fprintf(env, "Finished scanning %d objects [%v elapsed]\n",
				idx.Len(), time.Since(start).Truncate(10*time.Millisecond))

			rp.IndexKey, err = wiretype.Save(env.Context(), s.Files(), &wiretype.Object{
				Value: &wiretype.Object_Index{Index: index.Encode(idx)},
			})
			if err != nil {
				return fmt.Errorf("saving index: %w", err)
			}
			if err := rp.Save(env.Context(), key, true); err != nil {
				return err
			}
		}
		return nil
	})
}
