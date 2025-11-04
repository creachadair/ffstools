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
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"sync/atomic"
	"time"

	"github.com/creachadair/command"
	"github.com/creachadair/ffs/blob"
	"github.com/creachadair/ffs/filetree"
	"github.com/creachadair/ffs/index"
	"github.com/creachadair/ffstools/ffs/config"
	"github.com/creachadair/ffstools/lib/pbar"
	"github.com/creachadair/ffstools/lib/scanlib"
	"github.com/creachadair/flax"
	"github.com/creachadair/taskgroup"
)

var syncFlags struct {
	Target     string `flag:"to,Target store (required)"`
	Verbose    bool   `flag:"v,Enable verbose logging"`
	VVerbose   bool   `flag:"vv,PRIVATE:Enable detailed verbose logging"`
	NoIndex    bool   `flag:"no-index,Do not use cached indices"`
	NoRoot     bool   `flag:"no-root,Do not copy referenced root pointers"`
	RootPrefix string `flag:"root-prefix,Prefix target root names with this text"`
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

var Command = &command.C{
	Name: "sync",
	Usage: `@<file-key>[/path/...] ...
<root-key>[/path/...] ...`,
	Help: `Synchronize file trees between stores.

Transfer all the objects reachable from the specified file or root
paths into the given target store. By default, any roots mentioned
are also copied to the target; use --no-root to skip this step.
`,

	SetFlags: command.Flags(flax.MustBind, &syncFlags),
	Run:      command.Adapt(runSync),
}

func runSync(env *command.Env, sourceKeys ...string) error {
	if syncFlags.Target == "" {
		return env.Usagef("missing -to target store")
	}

	cfg := env.Config.(*config.Settings)
	return cfg.WithStore(env.Context(), func(src filetree.Store) error {
		return cfg.WithStoreAddress(env.Context(), syncFlags.Target, func(tgt filetree.Store) error {
			fmt.Fprintf(env, "Target store: %q\n", syncFlags.Target)

			// Find all the objects reachable from the specified starting points.
			worklist := scanlib.NewScanner(src.Files())
			var indices []*index.Index
			for _, elt := range sourceKeys {
				of, err := filetree.OpenPath(env.Context(), src, elt)
				if err != nil {
					return err
				}

				scanStart := time.Now()
				if of.Root != nil && of.Base == of.File {
					if of.Root.IndexKey != "" && !syncFlags.NoIndex {
						idx, err := config.LoadIndex(env.Context(), src.Files(), of.Root.IndexKey)
						if err != nil {
							return err
						}
						worklist.RootOnly(of.RootKey, of.Root)
						indices = append(indices, idx)
						dprintf(env, "Loaded cached index for %q (%d keys)\n", elt, idx.Len())
						continue
					}
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

			// If we loaded cached indices, fill the worklist with matching keys.
			if len(indices) != 0 {
				var numAdded int
				for key, err := range src.Files().List(env.Context(), "") {
					if err != nil {
						return err
					}
					for _, idx := range indices {
						if idx.Has(key) {
							worklist.Data(key)
							numAdded++
							break
						}
					}
				}
				dprintf(env, "Added %d reachable objects from %d indices\n", numAdded, len(indices))
			}

			fmt.Fprintf(env, "Found %d reachable objects\n", worklist.Len())
			if worklist.Len() == 0 {
				return errors.New("no matching objects")
			}

			// Remove from the worklist all objects already stored in the target
			// that are not scheduled for replacement. Objects marked as root (R)
			// or otherwise requiring replacement (+) are retained regardless.
			var nspan, nmiss int
			for span := range worklist.Chunks(512) {
				nspan++
				need, err := blob.SyncKeys(env.Context(), tgt.Files(), span)
				if err != nil {
					return err
				}
				nmiss += len(need)
				for _, key := range span {
					if !worklist.IsRoot(key) && !need.Has(key) {
						worklist.Remove(key)
					}
				}
			}
			dprintf(env, "Key scan processed %d spans, found %d missing keys\n", nspan, nmiss)
			fmt.Fprintf(env, "Have %d objects to copy\n", worklist.Len())

			var pb *pbar.Bar
			if worklist.Len() > 1000 {
				pb = pbar.New(env, int64(worklist.Len())).Start()
			}

			// Copy all remaining objects.
			start := time.Now()
			var nb int64

			ctx, cancel := context.WithCancel(env.Context())
			defer cancel()

			g, run := taskgroup.New(cancel).Limit(64)
			for key, tag := range worklist.All() {
				if ctx.Err() != nil {
					break
				} else if key == "" {
					continue
				}

				run(func() error {
					pb.Add(1)
					if tag == scanlib.Root && syncFlags.NoRoot {
						dprintf(env, "NOTE: Skipping root %q [--no-root]\n", key)
						return nil
					}
					defer atomic.AddInt64(&nb, 1)
					switch tag {
					case scanlib.Root:
						debug("- copying root %q", key)
						return moveBlob(ctx, src.Roots(), tgt.Roots(), key, syncFlags.RootPrefix+key, true)
					case scanlib.File:
						debug("- copying file %s", config.FormatKey(key))
						return copyBlob(ctx, src.Sync(), tgt.Sync(), key, false)
					case scanlib.Data, scanlib.Index:
						return copyBlob(ctx, src.Sync(), tgt.Sync(), key, false)
					default:
						panic("unknown tag " + string(tag))
					}
				})
			}
			cerr := g.Wait()
			if pb != nil {
				pb.Stop()
				fmt.Fprintln(env, " *")
			}
			fmt.Fprintf(env, "Copied %d objects [%v elapsed]\n",
				nb, time.Since(start).Truncate(10*time.Millisecond))
			return cerr
		})
	})
}

func copyBlob(ctx context.Context, src, tgt blob.KV, key string, replace bool) error {
	return moveBlob(ctx, src, tgt, key, key, replace)
}

func moveBlob(ctx context.Context, src, tgt blob.KV, oldKey, newKey string, replace bool) error {
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
