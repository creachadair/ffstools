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

// Package cmdgc implements the "ffs gc" subcommand.
package cmdgc

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"math/rand"
	"strings"
	"sync/atomic"
	"time"

	"github.com/creachadair/command"
	"github.com/creachadair/ffs/blob"
	"github.com/creachadair/ffs/file"
	"github.com/creachadair/ffs/file/root"
	"github.com/creachadair/ffs/index"
	"github.com/creachadair/ffstools/ffs/config"
	"github.com/creachadair/ffstools/lib/pbar"
	"github.com/creachadair/flax"
	"github.com/creachadair/mds/mapset"
	"github.com/creachadair/taskgroup"
)

var gcFlags struct {
	Force        bool          `flag:"force,Force collection on empty root list (DANGER)"`
	Limit        time.Duration `flag:"limit,Time limit for sweep phase (0=unlimited)"`
	Tasks        int           `flag:"nw,default=256,PRIVATE:Number of current sweep tasks"`
	RequireIndex bool          `flag:"require-index,Report an error if a root does not have an index"`
	Verbose      bool          `flag:"v,Enable verbose logging"`
}

var errSweepLimit = errors.New("sweep limit reached")

var Command = &command.C{
	Name: "gc",
	Help: `Garbage-collect objects not reachable from known roots.

If no roots are defined, an error is reported without making any changes
unless -force is set. This avoids accidentally deleting everything in a
store without roots.
`,

	SetFlags: command.Flags(flax.MustBind, &gcFlags),

	Run: command.Adapt(func(env *command.Env) error {
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
			fmt.Fprintf(env, "Begin GC of %d objects from %d roots\n", n, len(keys))
			dprintf(env, "Roots: %s\n", wrap(keys, 90, "  ", ", "))

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
					dprintf(env, "Loaded cached index for %q (%s)\n", key, config.FormatKey(rp.IndexKey))
					continue
				}

				// If an index is required, report an error.
				if gcFlags.RequireIndex {
					return fmt.Errorf("missing required index for %q", key)
				}

				// Otherwise, we need to compute the reachable set.
				// TODO(creachadair): Maybe cache the results here too.
				rf, err := rp.File(env.Context(), s)
				if err != nil {
					return fmt.Errorf("opening %q: %w", rp.FileKey, err)
				}
				idx.Add(rp.FileKey)

				dprintf(env, "Scanning data reachable from %q (%s)...\n",
					config.PrintableKey(key), config.FormatKey(rp.FileKey))
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
				dprintf(env, "Finished scanning %d objects [%v elapsed]\n",
					idx.Len(), time.Since(start).Truncate(10*time.Millisecond))
			}
			idxs = append(idxs, idx)

			// Sweep phase: Remove objects not indexed.
			ctx, cancel := context.WithCancelCause(env.Context())
			defer cancel(nil)
			if gcFlags.Limit > 0 {
				time.AfterFunc(gcFlags.Limit, func() { cancel(errSweepLimit) })
				fmt.Fprintf(env, "Begin sweep over %d objects (limit %v)...\n", n, gcFlags.Limit)
			} else {
				fmt.Fprintf(env, "Begin sweep over %d objects...\n", n)
			}

			// We want to have _some_ basic limit here, but it does not need to be
			// very tight.  Deletion rate limits are pretty forgiving.
			g, run := taskgroup.New(cancel).Limit(gcFlags.Tasks)

			start := time.Now()
			var numKeep, numDrop atomic.Int64
			pb := pbar.New(env, n).Start()
			for _, p := range shuffledSeeds() {
				pfx := string([]byte{p})
				run(func() error {
					return s.List(ctx, pfx, func(key string) error {
						if !strings.HasPrefix(key, pfx) {
							return blob.ErrStopListing
						}
						pb.Add(1)
						for _, idx := range idxs {
							if idx.Has(key) {
								numKeep.Add(1)
								return nil
							}
						}
						pb.SetMeta(numDrop.Add(1))
						if err := s.Delete(ctx, key); err != nil && !errors.Is(err, context.Canceled) {
							log.Printf("WARNING: delete key %s: %v", config.FormatKey(key), err)
						}
						return nil
					})
				})
			}
			serr := g.Wait()
			pb.Stop()
			fmt.Fprintln(env, " *")
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
	}),
}

func shuffledSeeds() []byte {
	m := make([]byte, 256)
	for i := range m {
		m[i] = byte(i)
	}
	rand.Shuffle(256, func(i, j int) {
		m[i], m[j] = m[j], m[i]
	})
	return m
}

func wrap(ss []string, n int, indent, sep string) string {
	var sb strings.Builder
	w := 0
	for i, s := range ss {
		sb.WriteString(s)
		if i+1 < len(ss) {
			sb.WriteString(sep)
		}
		w += len(s)
		if w >= n {
			sb.WriteString("\n" + indent)
			w = 0
		}
	}
	return sb.String()
}

func dprintf(w io.Writer, msg string, args ...any) {
	if gcFlags.Verbose {
		fmt.Fprintf(w, msg, args...)
	}
}
