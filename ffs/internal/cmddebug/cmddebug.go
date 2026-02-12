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

// Package cmddebug implements the "ffs debug" subcommand family.
package cmddebug

import (
	"context"
	"fmt"
	"os"

	"github.com/creachadair/command"
	"github.com/creachadair/ffs/blob"
	"github.com/creachadair/ffs/blob/memstore"
	"github.com/creachadair/ffs/file"
	"github.com/creachadair/ffs/file/root"
	"github.com/creachadair/ffs/filetree"
	"github.com/creachadair/ffstools/ffs/config"
	"github.com/creachadair/flax"
	"github.com/creachadair/mds/cache"
	"github.com/creachadair/mds/value"
	"github.com/creachadair/taskgroup"
)

var Command = &command.C{
	Name:     "debug",
	Help:     "Commands for low-level debugging.",
	Unlisted: true,

	Commands: []*command.C{
		{
			Name: "rewrite",
			Usage: `@<file-key>[/path/...] ...
<root-key>[/path/...] ...`,
			Help: `Rewrite file trees into a new store.

Recursively copy the specified file trees into another store.
Unlike sync, which copies the blobs as-written, rewrite completely
regenerates the blobs.

If an argument is a root pointer with no subordinate path, the
root is also copied to the target.`,

			SetFlags: command.Flags(flax.MustBind, &rewriteFlags),
			Run:      command.Adapt(runRewrite),
		},
		{
			Name:  "file-hash",
			Usage: `path ...`,
			Help:  "Compute the content hash of files in the local filesystem.",
			Run:   command.Adapt(runFileHash),
		},
	},
}

var rewriteFlags struct {
	Target string `flag:"to,Target store (required)"`
}

func runRewrite(env *command.Env, sourceKeys ...string) error {
	if rewriteFlags.Target == "" {
		return env.Usagef("mussing --to target store")
	}

	cfg := env.Config.(*config.Settings)
	return cfg.WithStore(env.Context(), func(src filetree.Store) error {
		return cfg.WithStoreAddress(env.Context(), rewriteFlags.Target, func(tgt filetree.Store) error {
			fmt.Fprintf(env, "Target store: %q\n", rewriteFlags.Target)

			for _, arg := range sourceKeys {
				pi, err := filetree.OpenPath(env.Context(), src, arg)
				if err != nil {
					return err
				}

				fmt.Fprintf(env, "Rewriting %q...\n", arg)
				c := cache.New(cache.LRU[string, *file.File]().WithLimit(1 << 20))
				rf, err := rewriteRecursive(env.Context(), pi.File, tgt, c)
				if err != nil {
					return err
				}
				rfKey, err := rf.Flush(env.Context())
				if err != nil {
					return err
				}

				if pi.Root != nil && pi.File == pi.Base {
					if pi.Root.IndexKey != "" {
						fmt.Fprintf(env, "WARNING: Root index %s not copied, it must be re-generated\n",
							config.FormatKey(pi.Root.IndexKey))
					}
					nr := root.New(tgt.Roots(), &root.Options{
						FileKey:     rf.Key(),
						Description: pi.Root.Description,
					})
					fmt.Fprintf(env, "Copying root %q...\n", pi.RootKey)
					if err := nr.Save(env.Context(), pi.RootKey); err != nil {
						return fmt.Errorf("save root: %w", err)
					}
				}
				fmt.Printf("src: %s\n", config.FormatKey(pi.File.Key()))
				fmt.Printf("dst: %s\n", config.FormatKey(rfKey))
			}
			return nil
		})
	})
}

func rewriteRecursive(ctx context.Context, f *file.File, tgt filetree.Store, seen *cache.Cache[string, *file.File]) (*file.File, error) {
	if sf, ok := seen.Get(f.Key()); ok {
		return sf, nil
	}
	nf := file.New(tgt.Files(), &file.NewOptions{
		Name:        f.Name(),
		Stat:        value.Ptr(f.Stat()),
		PersistStat: f.Stat().Persistent(),
	})
	var g taskgroup.Group
	for _, kid := range f.Child().Names() {
		kf, err := f.Open(ctx, kid)
		if err != nil {
			return nil, fmt.Errorf("open child: %w", err)
		}
		if kf.Child().Len() == 0 {
			g.Go(func() error {
				rkf, err := rewriteRecursive(ctx, kf, tgt, seen)
				if err != nil {
					return err
				}
				nf.Child().Set(kid, rkf)
				return nil
			})
		} else if rkf, err := rewriteRecursive(ctx, kf, tgt, seen); err != nil {
			g.Wait()
			return nil, err
		} else {
			nf.Child().Set(kid, rkf)
		}
	}
	if err := g.Wait(); err != nil {
		return nil, err
	}
	if d := f.Data(); d.Size() > 0 {
		if err := nf.SetData(ctx, f.Cursor(ctx)); err != nil {
			return nil, fmt.Errorf("copy data: %w", err)
		}
	}
	if xa := f.XAttr(); xa.Len() > 0 {
		nxa := nf.XAttr()
		for _, name := range xa.Names() {
			nxa.Set(name, xa.Get(name))
		}
	}

	// Note: Updating the children touches the file, so reset the modtime.
	nf.Stat().WithModTime(f.Stat().ModTime).Update()
	seen.Put(f.Key(), nf) // N.B. original file key, not the new one
	return nf, nil
}

func runFileHash(env *command.Env, paths ...string) error {
	kv := memstore.NewKV()
	for _, path := range paths {
		kv.Clear()
		f, err := os.Open(path)
		if err != nil {
			return err
		}

		cas := blob.CASFromKV(kv)
		cf := file.New(cas, nil)
		err = cf.SetData(env.Context(), f)
		f.Close()
		if err != nil {
			return fmt.Errorf("set data: %w", err)
		}
		h := cf.Data().Hash()
		if len(paths) != 1 {
			fmt.Print(path, "\t")
		}
		fmt.Println(config.FormatKey(string(h)))
	}
	return nil
}
