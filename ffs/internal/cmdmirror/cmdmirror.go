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

// Package cmdmirror implements the "ffs mirror" subcommand.
package cmdmirror

import (
	"cmp"
	"context"
	"fmt"
	"iter"
	"time"

	"github.com/creachadair/command"
	"github.com/creachadair/ffs/blob"
	"github.com/creachadair/ffs/filetree"
	"github.com/creachadair/ffstools/ffs/config"
	"github.com/creachadair/flax"
	"github.com/creachadair/taskgroup"
)

var mirrorFlags struct {
	Target  string `flag:"to,Target store (required unless --from is set)"`
	Source  string `flag:"from,Source store (required unless --to is set)"`
	NoRoots bool   `flag:"no-roots,Do not mirror root pointers"`
	NoFiles bool   `flag:"no-files,Do not mirror file trees"`
}

var Command = &command.C{
	Name: "mirror",
	Help: `Mirror files and roots between stores.

Transfer all the file and root objects between the source and target stores.
If --to=target is set, objects are copied from --store to the specified target;
if --from=source is set, objects are copied from source to the --store.
Exactly one of --to and --from must be set.`,

	SetFlags: command.Flags(flax.MustBind, &mirrorFlags),
	Run:      command.Adapt(runMirror),
}

func runMirror(env *command.Env) error {
	if (mirrorFlags.Target == "") == (mirrorFlags.Source == "") {
		return env.Usagef("exactly one of --from and --to must be set")
	}

	cfg := env.Config.(*config.Settings)
	otherSpec := cmp.Or(mirrorFlags.Target, mirrorFlags.Source)
	return cfg.WithStore(env.Context(), func(main filetree.Store) error {
		return cfg.WithStoreAddress(env.Context(), otherSpec, func(other filetree.Store) error {
			var src, tgt filetree.Store
			if mirrorFlags.Target != "" {
				fmt.Fprintf(env, "Target store: %q\n", mirrorFlags.Target)
				src, tgt = main, other
			} else {
				fmt.Fprintf(env, "Source store %q\n", mirrorFlags.Source)
				src, tgt = other, main
			}

			// If requested, copy roots.
			if mirrorFlags.NoRoots {
				fmt.Fprintln(env, "Skipping root pointers")
			} else {
				start := time.Now()
				var numRoots int
				for key, err := range src.Roots().List(env.Context(), "") {
					if err != nil {
						return err
					}
					if err := copyBlob(env.Context(), src.Roots(), tgt.Roots(), key, true); err != nil {
						return err
					}
					numRoots++
				}
				fmt.Fprintf(env, "Copied %d roots (%v elapsed)\n", numRoots, time.Since(start).Truncate(time.Millisecond))
			}

			// If requested, copy files.
			if mirrorFlags.NoFiles {
				fmt.Fprintln(env, "Skipping file objects")
			} else {
				ctx, cancel := context.WithCancel(env.Context())
				defer cancel()
				g, run := taskgroup.New(cancel).Limit(64)

				start := time.Now()
				var numBatches, numBlobs int
				err := forEachChunk(src.Sync().List(ctx, ""), 512, func(keys []string) error {
					if ctx.Err() != nil {
						return ctx.Err()
					}
					need, err := blob.SyncKeys(env.Context(), tgt.Sync(), keys)
					if err != nil {
						return err
					}
					numBatches++
					for key := range need {
						run(func() error {
							return copyBlob(env.Context(), src.Sync(), tgt.Sync(), key, true)
						})
						numBlobs++
					}
					numBlobs += len(need)
					return nil
				})
				if err != nil {
					return err
				} else if err := g.Wait(); err != nil {
					return err
				}
				fmt.Fprintf(env, "Copied %d objects (%d batches, %v elapsed)\n",
					numBlobs, numBatches, time.Since(start).Truncate(time.Millisecond))
			}
			return nil
		})
	})
}

func copyBlob(ctx context.Context, src, tgt blob.KV, key string, replace bool) error {
	bits, err := src.Get(ctx, key)
	if err != nil {
		return err
	}
	err = tgt.Put(ctx, blob.PutOptions{
		Key:     key,
		Data:    bits,
		Replace: replace,
	})
	if blob.IsKeyExists(err) {
		err = nil
	}
	return err
}

func forEachChunk[T any](seq iter.Seq2[T, error], n int, f func([]T) error) error {
	var cur []T
	for key, err := range seq {
		if err != nil {
			return err
		}
		cur = append(cur, key)
		if len(cur) == n {
			if err := f(cur); err != nil {
				return err
			}
			cur = cur[:0]
		}
	}
	if len(cur) != 0 {
		return f(cur)
	}
	return nil
}
