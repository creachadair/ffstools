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
	"github.com/creachadair/ffs/blob"
	"github.com/creachadair/ffs/file/root"
	"github.com/creachadair/ffs/file/wiretype"
	"github.com/creachadair/ffs/index"
	"github.com/creachadair/ffs/storage/prefixed"
	"github.com/creachadair/ffstools/ffs/config"
)

var indexFlags struct {
	Force bool
}

func runIndex(env *command.Env, keys []string) error {
	if len(keys) == 0 {
		return env.Usagef("missing required <root-key>")
	}

	cfg := env.Config.(*config.Settings)
	return cfg.WithStore(cfg.Context, func(s blob.CAS) error {
		n, err := s.(prefixed.CAS).Derive("").Len(cfg.Context)
		if err != nil {
			return err
		}
		for _, key := range keys {
			rp, err := root.Open(cfg.Context, config.Roots(s), key)
			if err != nil {
				return err
			}
			if rp.IndexKey != "" && !indexFlags.Force {
				fmt.Fprintf(env, "Root %q is already indexed\n", key)
				continue
			}
			fp, err := rp.File(cfg.Context, s)
			if err != nil {
				return err
			}

			fmt.Fprintf(env, "Scanning data reachable from %q (%x)...\n", key, rp.FileKey)
			scanned := make(map[string]bool)
			idx := index.New(int(n), &index.Options{FalsePositiveRate: 0.01})
			start := time.Now()
			if err := fp.Scan(cfg.Context, func(key string, isFile bool) bool {
				if isFile {
					if scanned[key] {
						return false // don't re-index repeats of the same file
					}
					scanned[key] = true
				}
				idx.Add(key)
				return true
			}); err != nil {
				return fmt.Errorf("scanning %q: %w", key, err)
			}
			fmt.Fprintf(env, "Finished scanning %d blobs [%v elapsed]\n",
				idx.Len(), time.Since(start).Truncate(10*time.Millisecond))

			rp.IndexKey, err = wiretype.Save(cfg.Context, s, &wiretype.Object{
				Value: &wiretype.Object_Index{Index: index.Encode(idx)},
			})
			if err != nil {
				return fmt.Errorf("saving index: %w", err)
			}
			if err := rp.Save(cfg.Context, key, true); err != nil {
				return err
			}
		}
		return nil
	})
}
