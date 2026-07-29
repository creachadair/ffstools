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

// Package cmdsync implements the "ffs sync" subcommand.
package cmdsync

import (
	"cmp"
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"slices"
	"time"

	"github.com/creachadair/command"
	"github.com/creachadair/ffs/blob"
	"github.com/creachadair/ffs/file"
	"github.com/creachadair/ffs/filetree"
	"github.com/creachadair/ffs/index"
	"github.com/creachadair/ffstools/ffs/config"
	"github.com/creachadair/ffstools/lib/pbar"
	"github.com/creachadair/flax"
	"github.com/creachadair/taskgroup"
)

var syncFlags struct {
	Target       string `flag:"to,Target store (required unless --from is set)"`
	Source       string `flag:"from,Source store (required unless --to is set)"`
	Quiet        bool   `flag:"q,Reduce diagnostic output"`
	Verbose      bool   `flag:"v,Enable verbose logging"`
	VVerbose     bool   `flag:"vv,PRIVATE:Enable detailed verbose logging"`
	NoIndex      bool   `flag:"no-index,Do not use cached indices"`
	NoRoot       bool   `flag:"no-root,Do not copy referenced root pointers"`
	RootPrefix   string `flag:"root-prefix,Prefix target root names with this text"`
	RequireIndex bool   `flag:"require-index,Report an error if a specified root does not have an index"`
}

func debug(msg string, args ...any) {
	if syncFlags.VVerbose {
		log.Printf(msg, args...)
	}
}

func dprintf(w io.Writer, msg string, args ...any) {
	if syncFlags.Verbose || syncFlags.VVerbose {
		fmt.Fprintf(w, msg, args...)
	}
}

func qprintf(w io.Writer, msg string, args ...any) {
	if !syncFlags.Quiet {
		fmt.Fprintf(w, msg, args...)
	}
}

var Command = &command.C{
	Name: "sync",
	Usage: `@<file-key>[/path/...] ...
<root-key>[/path/...] ...`,
	Help: `Synchronize file trees between stores.

Transfer all the objects reachable from the specified file or root paths
from one store into another. If --to=target is set, objects are copied
from --store to the specified target; if --from=source is set, objects are
copied from source to the --store. Exactly one of --to and --from must
be set.

By default, an argument that specifies a plain root name causes that root
(and its index, if one exists) to be copied along with the files reachable
from it. Use --no-root to skip copying the root and index, and only copy its
file tree.
`,

	SetFlags: command.Flags(flax.MustBind, &syncFlags),
	Run:      command.Adapt(runSync),
}

func runSync(env *command.Env, sourceKeys ...string) error {
	if (syncFlags.Target == "") == (syncFlags.Source == "") {
		return env.Usagef("exactly one of --from and --to must be set")
	}

	cfg := env.Config.(*config.Settings)
	otherSpec := cmp.Or(syncFlags.Target, syncFlags.Source)
	return cfg.WithStore(env.Context(), func(main filetree.Store) error {
		return cfg.WithStoreAddress(env.Context(), otherSpec, func(other filetree.Store) error {
			var src, tgt filetree.Store
			if syncFlags.Target != "" {
				qprintf(env, "Target store: %q\n", syncFlags.Target)
				src, tgt = main, other
			} else {
				qprintf(env, "Source store %q\n", syncFlags.Source)
				src, tgt = other, main
			}

			ctx, cancel := context.WithCancel(env.Context())
			defer cancel()
			g, run := taskgroup.New(cancel).Limit(64)

			// Find all the objects reachable from the specified starting points.
			// For an indexed root, capture the index and scan the whole corpus.
			// Otherwise, scan from the target file and copy any missing items.
			syncStart := time.Now()
			var indices []*index.Index
			var roots []*filetree.PathInfo
			var totalCopied int64
			for _, elt := range sourceKeys {
				of, err := src.OpenPath(env.Context(), elt)
				if err != nil {
					return err
				}

				scanStart := time.Now()
				if of.Root != nil && of.Base == of.File {
					roots = append(roots, of) // to be copied, below
					if syncFlags.RequireIndex && of.Root.IndexKey == "" {
						return fmt.Errorf("missing required index for %q", elt)
					} else if of.Root.IndexKey != "" && !syncFlags.NoIndex {
						idx, err := src.LoadIndex(env.Context(), of.Root.IndexKey)
						if err != nil {
							return err
						}
						dprintf(env, "Loaded cached index for %q (%d keys)\n", elt, idx.Len())
						indices = append(indices, idx)
						continue
					}
					qprintf(env, "Scanning data reachable from root %q ", of.RootKey)
				} else {
					qprintf(env, "Scanning data reachable from file %s ", filetree.FormatKey32(of.FileKey))
				}
				nk, err := scanAndCopy(ctx, of.File, src.Files(), tgt.Sync(), run)
				qprintf(env, "[%d copied, %v elapsed]\n", nk, time.Since(scanStart).Round(time.Millisecond))
				if err != nil {
					return err
				}
				totalCopied += int64(nk)
			}
			if err := g.Wait(); err != nil {
				return err
			}

			// If we loaded cached indices, copy any missing keys.
			if len(indices) != 0 {
				missing, err := findMissing(ctx, indices, src.Files(), tgt.Sync())
				if err != nil {
					return err
				}
				dprintf(os.Stderr, "Key scan found %d missing keys\n", len(missing))
				var pb *pbar.Bar
				if len(missing) > 1000 && !syncFlags.Quiet {
					pb = pbar.New(env, int64(len(missing))).Start()
				}
				for key := range missing {
					if ctx.Err() != nil {
						break
					}
					run(func() error {
						pb.Add(1)
						defer pb.AddMeta(1)
						return copyBlob(ctx, src.Sync(), tgt.Sync(), key, false)
					})
				}
				if err := g.Wait(); err != nil {
					return err
				}
				pb.Stop()
				dprintf(env, "Copied %d reachable objects from %d indices\n", len(missing), len(indices))
				totalCopied += int64(len(missing))
			}

			// Copy any roots (and associated index blobs) that were mentioned as starting points.
			for _, root := range roots {
				key := root.RootKey
				if syncFlags.NoRoot {
					dprintf(env, "NOTE: Skipping root %q [--no-root]\n", key)
					continue
				}
				debug("- copying root %q", key)
				if ik := root.Root.IndexKey; ik != "" {
					// Do the index first, so we don't copy a broken root pointer if it fails.
					if err := copyBlob(ctx, src.Files(), tgt.Sync(), ik, false); err != nil {
						return err
					}
					totalCopied++
				}
				if err := moveBlob(ctx, src.Roots(), tgt.Roots(), key, syncFlags.RootPrefix+key, true); err != nil {
					return err
				}
				totalCopied++
			}
			fmt.Fprintf(env, "Copied %d objects [%v elapsed]\n",
				totalCopied, time.Since(syncStart).Truncate(10*time.Millisecond))
			return nil
		})
	})
}

func copyBlob(ctx context.Context, src blob.KVCore, tgt blob.KV, key string, replace bool) error {
	return moveBlob(ctx, src, tgt, key, key, replace)
}

func moveBlob(ctx context.Context, src blob.KVCore, tgt blob.KV, oldKey, newKey string, replace bool) error {
	bits, err := src.Get(ctx, oldKey)
	if err != nil {
		return err
	}
	err = tgt.Put(ctx, blob.PutOptions{
		Key:     newKey,
		Data:    bits,
		Replace: replace,
	})
	if blob.IsKeyExists(err) {
		err = nil
	}
	return err
}

// scanAndCopy scans all the content-addressed blobs reachable from root in
// src, and copies any to tgt that are not already present there.
//
// Copies are executed concurrently via run.  It reports the total number of
// copies successfully issued, but the caller must wait for the taskgroup to
// settle before reporting success.
//
// Copyying during the scan, rather than separately, takes better advantage of
// a store cache, since the file being visited is likely to be still warm when
// we do the copy. This is especially helpful for remote stores, where the cost
// of re-fetching a blob faulted out may be substantial.
func scanAndCopy(ctx context.Context, root *file.File, src blob.KVCore, tgt blob.KV, run taskgroup.StartFunc) (int, error) {
	var seen, data blob.KeySet
	var check []string
	err := root.Scan(ctx, func(si file.ScanItem) error {
		if ctx.Err() != nil {
			return ctx.Err()
		} else if seen.Has(si.Key()) {
			return file.ErrSkipChildren // already visited
		}
		check = append(check[:0], si.Key())
		for _, dk := range si.Data().Keys() {
			if !data.Has(dk) {
				check = append(check, dk)
				data.Add(dk)
			}
		}
		need, err := blob.SyncKeys(ctx, tgt, check)
		if err != nil {
			return err
		}
		for missing := range need {
			run(func() error {
				return copyBlob(ctx, src, tgt, missing, false)
			})
		}
		seen.Add(si.Key())
		return nil
	})
	return len(seen) + len(data), err
}

// findMissing reports the set of all keys in src mentioned by one of the
// indices, but not present in tgt.
func findMissing(ctx context.Context, indices []*index.Index, src blob.KVCore, tgt blob.KV) (blob.KeySet, error) {
	var want blob.KeySet
	for key, err := range src.List(ctx, "") {
		if err != nil {
			return nil, err
		}
		for _, idx := range indices {
			if idx.Has(key) {
				want.Add(key)
				break
			}
		}
	}
	for ch := range slices.Chunk(want.Slice(), 64) {
		if ctx.Err() != nil {
			break
		}
		have, err := tgt.Has(ctx, ch...)
		if err != nil {
			return nil, err
		}
		want.RemoveAll(have)
	}
	return want, nil
}
