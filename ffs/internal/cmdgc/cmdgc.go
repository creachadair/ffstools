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
	"strings"
	"sync/atomic"
	"time"

	"github.com/creachadair/command"
	"github.com/creachadair/ffs/blob"
	"github.com/creachadair/ffs/file"
	"github.com/creachadair/ffs/file/root"
	"github.com/creachadair/ffs/filetree"
	"github.com/creachadair/ffs/index"
	"github.com/creachadair/ffstools/ffs/config"
	"github.com/creachadair/ffstools/lib/pbar"
	"github.com/creachadair/flax"
	"github.com/creachadair/mds/mapset"
	"github.com/creachadair/taskgroup"
)

var gcFlags struct {
	Force        bool `flag:"force,Force collection on empty root list (DANGER)"`
	Tasks        int  `flag:"nw,default=64,PRIVATE:Number of concurrent sweep tasks"`
	RequireIndex bool `flag:"require-index,Report an error if a root does not have an index"`
	Verbose      bool `flag:"v,Enable verbose logging"`

	// The expensive part of a GC is deleting the keys, which in cloud storage
	// are often heavily rate-limited. We want to proceed concurrently to the
	// extent practical, but there are diminishing returns.
}

var Command = &command.C{
	Name: "gc",
	Help: `Garbage-collect objects not reachable from known roots.

If no roots are defined, an error is reported without making any changes
unless --force is set. This avoids accidentally deleting everything in a
store without roots.
`,

	SetFlags: command.Flags(flax.MustBind, &gcFlags),

	Run: command.Adapt(func(env *command.Env) error {
		cfg := env.Config.(*config.Settings)
		return cfg.WithStore(env.Context(), func(s filetree.Store) error {
			var keys []string
			for key, err := range s.Roots().List(env.Context(), "") {
				if err != nil {
					return fmt.Errorf("listing roots: %w", err)
				}
				keys = append(keys, key)
			}

			if len(keys) == 0 && !gcFlags.Force {
				return errors.New("there are no root keys defined")
			} else if len(keys) == 0 {
				fmt.Fprint(env, `>> WARNING <<
* No root keys found!
* Proceeding with collection anyway because --force is set

`)
			}

			n, err := s.Files().Len(env.Context())
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
			for _, key := range keys {
				rp, err := root.Open(env.Context(), s.Roots(), key)
				if err != nil {
					return fmt.Errorf("opening %q: %w", key, err)
				}

				// If this root has a cached index, use that instead of scanning.
				if rp.IndexKey != "" {
					rpi, err := config.LoadIndex(env.Context(), s.Files(), rp.IndexKey)
					if err != nil {
						return err
					}
					idxs = append(idxs, rpi)
					idx.Add(rp.IndexKey)
					dprintf(env, "Loaded cached index for %q (%d keys, %s)\n",
						key, rpi.Stats().NumKeys, config.FormatKey(rp.IndexKey),
					)
					continue
				}

				// If an index is required, report an error.
				if gcFlags.RequireIndex {
					return fmt.Errorf("missing required index for %q", key)
				}

				// Otherwise, we need to compute the reachable set.
				// TODO(creachadair): Maybe cache the results here too.
				rf, err := rp.File(env.Context(), s.Files())
				if err != nil {
					return fmt.Errorf("opening %q: %w", rp.FileKey, err)
				}
				idx.Add(rp.FileKey)

				dprintf(env, "Scanning data reachable from %q (%s)...\n",
					config.PrintableKey(key), config.FormatKey(rp.FileKey))

				// Avoid re-scanning repeats of the same file. But note: We do not
				// want to use the index for this, as it is possible it may have a
				// false positive. In that case we would incorrectly skip the file,
				// so we want a true set.
				scannedFiles := mapset.New[string]()

				start := time.Now()
				if err := rf.Scan(env.Context(), func(si file.ScanItem) bool {
					key := si.Key()
					if scannedFiles.Has(key) {
						return false // already scanned
					}
					scannedFiles.Add(key)
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

			hasKey := func(key string) bool {
				for _, idx := range idxs {
					if idx.Has(key) {
						return true
					}
				}
				return false
			}

			// Sweep phase: Remove objects not indexed.
			ctx, cancel := context.WithCancelCause(env.Context())
			defer cancel(nil)
			fmt.Fprintf(env, "Begin sweep over %d objects\n", n)

			g, run := taskgroup.New(cancel).Limit(gcFlags.Tasks)

			start := time.Now()
			var numKeep, numDrop atomic.Int64

			// Sweep phase 1: Collect all the keys eligible for deletion.
			var toDrop mapset.Set[string]
			for key, err := range s.Files().List(ctx, "") {
				if err != nil {
					return err
				}

				if hasKey(key) {
					numKeep.Add(1)
					continue
				}
				toDrop.Add(key)
			}

			if !toDrop.IsEmpty() {
				fmt.Fprintf(env, "Found %d objects to delete\n", toDrop.Len())

				// Sweep phase 2: Delete all the eligible keys. This will be the bulk
				// of the work, for stores with expensive backends (e.g., cloud storage).
				pb := pbar.New(env, int64(toDrop.Len())).Start()
				for key := range toDrop {
					if ctx.Err() != nil {
						break
					}
					run.Go(func() error {
						pb.Add(1)
						err := s.Files().Delete(ctx, key)
						if err == nil || blob.IsKeyNotFound(err) {
							pb.SetMeta(numDrop.Add(1))
							return nil
						} else if !errors.Is(err, context.Canceled) {
							log.Printf("WARNING: delete key %s: %v", config.FormatKey(key), err)
						}
						return err
					})
				}

				// Clean up and report.
				serr := g.Wait()
				pb.Stop()
				fmt.Fprintln(env, " *")
				if serr != nil {
					return fmt.Errorf("sweeping failed: %w", serr)
				}
			}
			fmt.Fprintf(env, "GC complete: keep %d, drop %d [%v elapsed]\n",
				numKeep.Load(), numDrop.Load(), time.Since(start).Truncate(10*time.Millisecond))
			return nil
		})
	}),
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
