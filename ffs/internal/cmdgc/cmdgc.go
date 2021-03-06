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

package cmdgc

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/creachadair/command"
	"github.com/creachadair/ffs/blob"
	"github.com/creachadair/ffs/file/root"
	"github.com/creachadair/ffs/file/wiretype"
	"github.com/creachadair/ffs/index"
	"github.com/creachadair/ffstools/ffs/config"
	"github.com/creachadair/taskgroup"
)

var gcFlags struct {
	Force bool
}

var Command = &command.C{
	Name: "gc",
	Help: `Garbage-collect blobs not reachable from known roots.

If no roots are defined, an error is reported without making any changes
unless -force is set. This avoids accidentally deleting everything in a
store without roots.
`,

	SetFlags: func(_ *command.Env, fs *flag.FlagSet) {
		fs.BoolVar(&gcFlags.Force, "force", false, "Force collection on empty root list (DANGER)")
	},

	Run: func(env *command.Env, args []string) error {
		if len(args) != 0 {
			return env.Usagef("extra arguments after command")
		}

		cfg := env.Config.(*config.Settings)
		ctx, cancel := context.WithCancel(cfg.Context)
		return cfg.WithStore(cfg.Context, func(s blob.CAS) error {
			var keys []string
			if err := config.Roots(s).List(cfg.Context, "", func(key string) error {
				keys = append(keys, key)
				return nil
			}); err != nil {
				return fmt.Errorf("listing roots: %w", err)
			}

			if len(keys) == 0 && !gcFlags.Force {
				return errors.New("there are no root keys defined")
			} else if len(keys) == 0 {
				fmt.Fprint(env, `>> WARNING <<
* No root keys found!
* Proceeding with collection anyway because -force is set

`)
			}

			n, err := s.Len(ctx)
			if err != nil {
				return err
			} else if n == 0 {
				return errors.New("the store is empty")
			}
			var idxs []*index.Index
			idx := index.New(int(n), &index.Options{FalsePositiveRate: 0.01})
			fmt.Fprintf(env, "Begin GC of %d blobs, roots=%+q\n", n, keys)

			// Mark phase: Scan all roots.
			for i := 0; i < len(keys); i++ {
				key := keys[i]
				rp, err := root.Open(cfg.Context, config.Roots(s), key)
				if err != nil {
					return fmt.Errorf("opening %q: %w", key, err)
				}
				idx.Add(key)

				// If this root has a cached index, use that instead of scanning.
				if rp.IndexKey != "" {
					var obj wiretype.Object
					if err := wiretype.Load(cfg.Context, s, rp.IndexKey, &obj); err != nil {
						return fmt.Errorf("loading index: %w", err)
					}
					ridx := obj.GetIndex()
					if ridx == nil {
						return fmt.Errorf("no index in %x", rp.IndexKey)
					}

					rpi, err := index.Decode(ridx)
					if err != nil {
						return fmt.Errorf("decoding index for %q: %w", key, err)
					}
					idxs = append(idxs, rpi)
					idx.Add(rp.IndexKey)
					fmt.Fprintf(env, "Loaded cached index for %q (%x)\n", key, rp.IndexKey)
					continue
				}

				// Otherwise, we need to compute the reachable set.
				// TODO(creachadair): Maybe cache the results here too.
				rf, err := rp.File(cfg.Context, s)
				if err != nil {
					return fmt.Errorf("opening %q: %w", rp.FileKey, err)
				}
				idx.Add(rp.FileKey)

				fmt.Fprintf(env, "Scanning data reachable from %q (%x)...\n",
					config.PrintableKey(key), rp.FileKey)
				start := time.Now()
				var numKeys int
				if err := rf.Scan(cfg.Context, func(key string, isFile bool) bool {
					numKeys++
					idx.Add(key)
					return true
				}); err != nil {
					return fmt.Errorf("scanning %q: %w", key, err)
				}
				fmt.Fprintf(env, "Finished scanning %d blobs [%v elapsed]\n",
					numKeys, time.Since(start).Truncate(10*time.Millisecond))
			}
			idxs = append(idxs, idx)

			// Sweep phase: Remove blobs not indexed.
			g, run := taskgroup.New(taskgroup.Trigger(cancel)).Limit(256)

			fmt.Fprintf(env, "Begin sweep over %d blobs...\n", n)
			start := time.Now()
			var numKeep, numDrop uint32
			g.Go(func() error {
				defer fmt.Fprintln(env, "*")
				return s.List(cfg.Context, "", func(key string) error {
					run(func() error {
						for _, idx := range idxs {
							if idx.Has(key) {
								atomic.AddUint32(&numKeep, 1)
								return nil
							}
						}
						v := atomic.AddUint32(&numDrop, 1)
						if v%50 == 0 {
							fmt.Fprint(env, ".")
						}
						return s.Delete(ctx, key)
					})
					return nil
				})
			})
			fmt.Fprintln(env, "All key ranges listed, waiting for cleanup...")
			if err := g.Wait(); err != nil {
				return fmt.Errorf("sweeping failed: %w", err)
			}
			fmt.Fprintf(env, "GC complete: keep %d, drop %d [%v elapsed]\n",
				numKeep, numDrop, time.Since(start).Truncate(10*time.Millisecond))
			return nil
		})
	},
}
