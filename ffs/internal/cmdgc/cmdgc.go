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
	"log"
	"strings"
	"sync/atomic"
	"time"

	"github.com/creachadair/command"
	"github.com/creachadair/ffs/blob"
	"github.com/creachadair/ffs/file"
	"github.com/creachadair/ffs/file/root"
	"github.com/creachadair/ffs/index"
	"github.com/creachadair/ffstools/ffs/config"
	"github.com/creachadair/ffstools/ffs/internal/pbar"
	"github.com/creachadair/flax"
	"github.com/creachadair/mds/mapset"
	"github.com/creachadair/taskgroup"
)

var gcFlags struct {
	Force bool          `flag:"force,Force collection on empty root list (DANGER)"`
	Limit time.Duration `flag:"limit,Time limit for sweep phase (0=unlimited)"`
}

var errSweepLimit = errors.New("sweep limit reached")

var Command = &command.C{
	Name: "gc",
	Help: `Garbage-collect objects not reachable from known roots.

If no roots are defined, an error is reported without making any changes
unless -force is set. This avoids accidentally deleting everything in a
store without roots.
`,

	SetFlags: func(_ *command.Env, fs *flag.FlagSet) { flax.MustBind(fs, &gcFlags) },

	Run: func(env *command.Env) error {
		if len(env.Args) != 0 {
			return env.Usagef("extra arguments after command")
		}

		cfg := env.Config.(*config.Settings)
		return cfg.WithStore(env.Context(), func(s config.CAS) error {
			var keys []string
			if err := s.Roots().List(env.Context(), "", func(key string) error {
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

			n, err := s.Len(env.Context())
			if err != nil {
				return err
			} else if n == 0 {
				return errors.New("the store is empty")
			}
			var idxs []*index.Index
			idx := index.New(int(n), &index.Options{FalsePositiveRate: 0.01})
			fmt.Fprintf(env, "Begin GC of %d objects, roots=%+q\n", n, keys)

			// Mark phase: Scan all roots.
			for i := 0; i < len(keys); i++ {
				key := keys[i]
				rp, err := root.Open(env.Context(), s.Roots(), key)
				if err != nil {
					return fmt.Errorf("opening %q: %w", key, err)
				}
				idx.Add(key)

				// If this root has a cached index, use that instead of scanning.
				if rp.IndexKey != "" {
					rpi, err := config.LoadIndex(env.Context(), s, rp.IndexKey)
					if err != nil {
						return err
					}
					idxs = append(idxs, rpi)
					idx.Add(rp.IndexKey)
					fmt.Fprintf(env, "Loaded cached index for %q (%x)\n", key, rp.IndexKey)
					continue
				}

				// Otherwise, we need to compute the reachable set.
				// TODO(creachadair): Maybe cache the results here too.
				rf, err := rp.File(env.Context(), s)
				if err != nil {
					return fmt.Errorf("opening %q: %w", rp.FileKey, err)
				}
				idx.Add(rp.FileKey)

				fmt.Fprintf(env, "Scanning data reachable from %q (%x)...\n",
					config.PrintableKey(key), rp.FileKey)
				scanned := mapset.New[string]()
				start := time.Now()
				if err := rf.Scan(env.Context(), func(si file.ScanItem) bool {
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
			}
			idxs = append(idxs, idx)

			// Sweep phase: Remove objects not indexed.
			ctx, cancel := context.WithCancelCause(env.Context())
			defer cancel(nil)
			if gcFlags.Limit > 0 {
				fmt.Fprintf(env, "- sweep limit %v\n", gcFlags.Limit)
				time.AfterFunc(gcFlags.Limit, func() { cancel(errSweepLimit) })
			}
			g, run := taskgroup.New(taskgroup.Listen(cancel)).Limit(64)

			fmt.Fprintf(env, "Begin sweep over %d objects...\n", n)
			start := time.Now()
			var numKeep, numDrop atomic.Int64
			pb := pbar.New(env, n).Start()
			for i := 0; i < 256; i++ {
				pfx := string([]byte{byte(i)})
				g.Go(func() error {
					return s.List(ctx, pfx, func(key string) error {
						if !strings.HasPrefix(key, pfx) {
							return blob.ErrStopListing
						}
						pb.Add(1)
						run(taskgroup.NoError(func() {
							for _, idx := range idxs {
								if idx.Has(key) {
									numKeep.Add(1)
									return
								}
							}
							pb.SetMeta(numDrop.Add(1))
							if err := s.Delete(ctx, key); err != nil && !errors.Is(err, context.Canceled) {
								log.Printf("WARNING: delete key %x: %v", key, err)
							}
						}))
						return nil
					})
				})
			}
			serr := g.Wait()
			pb.Stop()
			fmt.Fprintln(env, "*")
			if serr != nil {
				if errors.Is(context.Cause(ctx), errSweepLimit) {
					fmt.Fprintln(env, "(sweep limit reached)")
				} else {
					return fmt.Errorf("sweeping failed: %w", serr)
				}
			}
			fmt.Fprintf(env, "GC complete: keep %d, drop %d [%v elapsed]\n",
				numKeep.Load(), numDrop.Load(), time.Since(start).Truncate(10*time.Millisecond))
			return nil
		})
	},
}
